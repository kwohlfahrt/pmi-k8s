use core::ffi;
use std::{io, mem, net::SocketAddr, slice, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net,
    sync::oneshot,
    time,
};

use crate::{
    ModexError,
    peer::PeerDiscovery,
    pmix::{char_to_u8, globals, sys, u8_to_char},
};

unsafe extern "C" fn response(
    status: sys::pmix_status_t,
    data: *mut std::ffi::c_char,
    sz: usize,
    cbdata: *mut std::ffi::c_void,
) {
    assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
    let data = if !data.is_null() {
        // SAFETY: Data is a pointer to [ffi::c_char; sz], and we have checked
        // for `null` ourselves.
        let slice = unsafe { slice::from_raw_parts(data, sz) };
        // Data is owned by PMIx, so we must copy
        char_to_u8(slice).to_vec()
    } else {
        Vec::new()
    };

    // SAFETY: We created `cbdata`` in `NetModex::respond`, from `oneshot::Sender<Vec<u8>>`
    let tx = *unsafe { Box::from_raw(cbdata as *mut oneshot::Sender<Vec<u8>>) };

    // If the receiver is dropped, there is nothing we have left to do.
    tx.send(data).unwrap_or_default()
}

type RequestFn = unsafe extern "C" fn(
    proc: *const sys::pmix_proc_t,
    cbfunc: sys::pmix_dmodex_response_fn_t,
    cbdata: *mut ffi::c_void,
) -> sys::pmix_status_t;

pub struct NetModex<'a, D: PeerDiscovery> {
    discovery: &'a D,
    listener: net::TcpListener,
    nproc: u16,
    request_fn: RequestFn,
}

impl<'a, D: PeerDiscovery> NetModex<'a, D> {
    pub async fn new(
        addr: SocketAddr,
        discovery: &'a D,
        nproc: u16,
    ) -> Result<Self, ModexError<D::Error>> {
        let listener = net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            discovery,
            nproc,
            request_fn: sys::PMIx_server_dmodex_request,
        })
    }

    #[cfg(test)]
    async fn with_mock_request(
        addr: SocketAddr,
        discovery: &'a D,
        nproc: u16,
        request_fn: RequestFn,
    ) -> io::Result<Self> {
        let listener = net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            discovery,
            nproc,
            request_fn,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        #[allow(clippy::unwrap_used, reason = "We know we have a socket bound")]
        self.listener.local_addr().unwrap()
    }

    fn serialize_proc(proc: sys::pmix_proc_t) -> Vec<u8> {
        let mut s = Vec::with_capacity(mem::size_of::<sys::pmix_proc_t>());
        s.extend_from_slice(char_to_u8(&proc.nspace));
        s.extend_from_slice(&proc.rank.to_be_bytes());
        s
    }

    fn parse_proc(buf: [u8; mem::size_of::<sys::pmix_proc_t>()]) -> sys::pmix_proc_t {
        let (nspace, rank) = buf.split_at(mem::size_of::<sys::pmix_nspace_t>());
        #[allow(clippy::unwrap_used, reason = "Sizes are statically known")]
        let rank = u32::from_be_bytes(rank.try_into().unwrap());
        #[allow(clippy::unwrap_used, reason = "Sizes are statically known")]
        let nspace = u8_to_char(nspace).try_into().unwrap();
        sys::pmix_proc_t { rank, nspace }
    }

    async fn request_data(&self, proc: sys::pmix_proc_t) -> Result<Vec<u8>, ModexError<D::Error>> {
        assert!(proc.rank <= sys::PMIX_RANK_VALID);
        let req = Self::serialize_proc(proc);

        let node_rank = proc.rank / self.nproc as u32;
        let addr = self
            .discovery
            .peer(node_rank)
            .await
            .map_err(ModexError::Peer)?;

        let mut s = loop {
            match net::TcpStream::connect(addr).await {
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    // TODO: Proper backoff
                    time::sleep(Duration::from_millis(250)).await
                }
                r => break r,
            }
        }?;
        s.write_all(&req).await?;
        let mut data = Vec::new();
        s.read_to_end(&mut data).await?;
        Ok(data)
    }

    pub async fn request(
        &self,
        proc: sys::pmix_proc_t,
        callback: globals::ModexCallback,
    ) -> Result<(), ModexError<D::Error>> {
        let (Some(cbfunc), cbdata) = callback else {
            return Ok(());
        };
        let acc = Box::new(self.request_data(proc).await?);
        let data = u8_to_char(&acc);

        // TODO: Create ModexCallback wrapper that handles this.
        // SAFETY: `data` lives as long as `acc`, which is freed by libpmix using `release_vec_u8`.
        unsafe {
            cbfunc(
                sys::PMIX_SUCCESS as sys::pmix_status_t,
                data.as_ptr(),
                data.len(),
                cbdata,
                Some(globals::release_vec_u8),
                Box::into_raw(acc) as *mut ffi::c_void,
            )
        };
        Ok(())
    }

    async fn respond(&self, mut c: net::TcpStream) -> Result<(), ModexError<D::Error>> {
        let mut buf = [0; _];
        c.read_exact(&mut buf).await?;
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        let proc = Self::parse_proc(buf);
        let tx = Box::new(tx);

        // SAFETY: `request_fn` is PMIx_server_dmodex_request outside of tests.
        // `response` unwraps `cbdata` into oneshot::Sender<Vec<u8>>.
        let status = unsafe {
            (self.request_fn)(&proc, Some(response), Box::into_raw(tx) as *mut ffi::c_void)
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);

        let data = rx.await.expect("PMIx did not return modex response");
        c.write_all(&data).await?;
        Ok(())
    }

    pub async fn serve(&self) -> Result<!, ModexError<D::Error>> {
        loop {
            // TODO: Process incoming requests in parallel
            let (c, _) = self.listener.accept().await?;
            self.respond(c).await?
        }
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used, clippy::panic, clippy::undocumented_unsafe_blocks)]
    use crate::peer::DirectoryPeers;
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

        let mut data: [ffi::c_char; _] = [1, 2, 3];
        let status = sys::PMIX_SUCCESS as sys::pmix_status_t;
        unsafe { cbfunc(status, data.as_mut_ptr(), data.len(), cbdata) };
        sys::PMIX_SUCCESS as sys::pmix_status_t
    }

    #[tokio::test]
    async fn test_modex() {
        let nproc = 4;

        let tmpdir = TempDir::new("modex-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), 2);
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let sender = NetModex::new(addr, &discovery, nproc).await.unwrap();
        let responder = NetModex::with_mock_request(addr, &discovery, nproc, request_fn)
            .await
            .unwrap();
        discovery.register(&sender.addr()).unwrap();
        discovery.register(&responder.addr()).unwrap();

        let proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank: nproc as u32,
        };
        let req = pin!(sender.request_data(proc));
        let serve = pin!(responder.serve());
        let resp = select(req, serve).await;
        let Either::Left((Ok(data), _)) = resp else {
            panic!("expected response");
        };
        assert_eq!(data, vec![1, 2, 3]);
    }
}
