use core::ffi;
use std::{io, mem, net::SocketAddr, slice, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net,
    sync::oneshot,
    time,
};

use crate::{
    peer::dir::PeerDiscovery,
    pmix::{globals, sys},
};

unsafe extern "C" fn response(
    status: sys::pmix_status_t,
    data: *mut std::ffi::c_char,
    sz: usize,
    cbdata: *mut std::ffi::c_void,
) {
    assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
    let data = if !data.is_null() {
        // data is owned by PMIx library, so we must copy.
        let slice = unsafe { slice::from_raw_parts(data, sz) };
        slice.to_vec()
    } else {
        Vec::new()
    };

    let tx = *unsafe { Box::from_raw(cbdata as *mut oneshot::Sender<Vec<u8>>) };
    tx.send(data).unwrap();
}

type RequestFn = unsafe extern "C" fn(
    proc: *const sys::pmix_proc_t,
    cbfunc: sys::pmix_dmodex_response_fn_t,
    cbdata: *mut ffi::c_void,
) -> sys::pmix_status_t;

pub struct NetModex<'a> {
    discovery: &'a PeerDiscovery<'a>,
    listener: net::TcpListener,
    nproc: u16,
    request_fn: RequestFn,
}

impl<'a> NetModex<'a> {
    pub async fn new(addr: SocketAddr, discovery: &'a PeerDiscovery<'a>, nproc: u16) -> Self {
        let listener = net::TcpListener::bind(addr).await.unwrap();
        Self {
            listener,
            discovery,
            nproc,
            request_fn: sys::PMIx_server_dmodex_request,
        }
    }

    #[cfg(test)]
    async fn with_mock_request(
        addr: SocketAddr,
        discovery: &'a PeerDiscovery<'a>,
        nproc: u16,
        request_fn: RequestFn,
    ) -> Self {
        let listener = net::TcpListener::bind(addr).await.unwrap();
        Self {
            listener,
            discovery,
            nproc,
            request_fn,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    fn serialize_proc(proc: sys::pmix_proc_t) -> Vec<u8> {
        let mut s = Vec::with_capacity(mem::size_of::<sys::pmix_proc_t>());
        s.extend_from_slice(&proc.nspace);
        s.extend_from_slice(&proc.rank.to_be_bytes());
        s
    }

    fn parse_proc(buf: [u8; mem::size_of::<sys::pmix_proc_t>()]) -> sys::pmix_proc_t {
        let (nspace, rank) = buf.split_at(mem::size_of::<sys::pmix_nspace_t>());
        let rank = u32::from_be_bytes(rank.try_into().unwrap());
        let nspace = nspace.try_into().unwrap();
        sys::pmix_proc_t { rank, nspace }
    }

    async fn request_data(&self, proc: sys::pmix_proc_t) -> Vec<u8> {
        assert!(proc.rank <= sys::PMIX_RANK_VALID);
        let req = Self::serialize_proc(proc);

        let node_rank = proc.rank / self.nproc as u32;
        let addr = self.discovery.peer(node_rank).await;

        let mut s = loop {
            match net::TcpStream::connect(addr).await {
                Ok(s) => break s,
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    // TODO: Proper backoff
                    time::sleep(Duration::from_millis(250)).await
                }
                Err(e) => panic!("unexpected send error: {e}"),
            }
        };
        s.write_all(&req).await.unwrap();
        let mut data = Vec::new();
        s.read_to_end(&mut data).await.unwrap();
        data
    }

    pub async fn request(&self, proc: sys::pmix_proc_t, callback: globals::ModexCallback) {
        let (Some(cbfunc), cbdata) = callback else {
            return;
        };
        let acc = Box::new(self.request_data(proc).await);
        unsafe {
            cbfunc(
                sys::PMIX_SUCCESS as sys::pmix_status_t,
                acc.as_ptr(),
                acc.len(),
                cbdata,
                Some(globals::release_vec_u8),
                Box::into_raw(acc) as *mut ffi::c_void,
            )
        }
    }

    async fn respond(&self, mut c: net::TcpStream) {
        let mut buf = [0; _];
        c.read_exact(&mut buf).await.unwrap();
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        let proc = Self::parse_proc(buf);
        let tx = Box::new(tx);

        let status = unsafe {
            (self.request_fn)(&proc, Some(response), Box::into_raw(tx) as *mut ffi::c_void)
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);

        let data = rx.await.unwrap();
        c.write_all(&data).await.unwrap()
    }

    pub async fn serve(&self) {
        while let Ok((c, _)) = self.listener.accept().await {
            // TODO: Process incoming requests in parallel
            self.respond(c).await
        }
    }
}

#[cfg(test)]
mod test {
    use std::{net::Ipv4Addr, pin::pin};

    use super::*;
    use futures::future::{Either, select};
    use tempdir::TempDir;

    unsafe extern "C" fn request_fn(
        _proc: *const sys::pmix_proc_t,
        cbfunc: sys::pmix_dmodex_response_fn_t,
        cbdata: *mut ffi::c_void,
    ) -> sys::pmix_status_t {
        let Some(cbfunc) = cbfunc else {
            return sys::PMIX_SUCCESS as sys::pmix_status_t;
        };

        let mut data: [u8; _] = [1, 2, 3];
        let status = sys::PMIX_SUCCESS as sys::pmix_status_t;
        unsafe { cbfunc(status, data.as_mut_ptr(), data.len(), cbdata) };
        sys::PMIX_SUCCESS as sys::pmix_status_t
    }

    #[tokio::test]
    async fn test_modex() {
        let nproc = 4;

        let tmpdir = TempDir::new("modex-test").unwrap();
        let discovery = PeerDiscovery::new(tmpdir.path(), 2);
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let sender = NetModex::new(addr, &discovery, nproc).await;
        let responder = NetModex::with_mock_request(addr, &discovery, nproc, request_fn).await;
        discovery.register(&sender.addr(), 0);
        discovery.register(&responder.addr(), 1);

        let proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank: nproc as u32 * 1,
        };
        let req = pin!(sender.request_data(proc));
        let serve = pin!(responder.serve());
        let resp = select(req, serve).await;
        let Either::Left((data, _)) = resp else {
            panic!("expected response");
        };
        assert_eq!(data, vec![1, 2, 3]);
    }
}
