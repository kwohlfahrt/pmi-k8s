use std::ffi;
use std::{mem, net::SocketAddr};

use futures::{StreamExt, TryStreamExt, future::join};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net,
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};

use crate::net::connect_peer;
use crate::{
    ModexError,
    peer::{Endpoint, PeerDiscovery},
    pmix::{
        char_to_u8,
        globals::{self, DirectModexEvent},
        slice_from_raw_parts, sys, u8_to_char,
    },
};

unsafe extern "C" fn response(
    status: sys::pmix_status_t,
    data: *mut std::ffi::c_char,
    sz: usize,
    cbdata: *mut std::ffi::c_void,
) {
    assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
    // SAFETY: Data is owned by PMIx and free'd after this function, so we must
    // copy before returning.
    let data = unsafe { slice_from_raw_parts(data, sz) };
    let data = char_to_u8(data).to_vec();

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
    request_fn: RequestFn,
}

impl<'a, D: PeerDiscovery> NetModex<'a, D> {
    pub async fn new(addr: SocketAddr, discovery: &'a D) -> Result<Self, ModexError<D::Error>> {
        Self::with_request_fn(addr, discovery, sys::PMIx_server_dmodex_request).await
    }

    async fn with_request_fn(
        addr: SocketAddr,
        discovery: &'a D,
        request_fn: RequestFn,
    ) -> Result<Self, ModexError<D::Error>> {
        let listener = net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            discovery,
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

    async fn request_data(
        discovery: &'a D,
        proc: sys::pmix_proc_t,
    ) -> Result<Vec<u8>, ModexError<D::Error>> {
        let req = Self::serialize_proc(proc);
        let addr = discovery
            .peer(&proc, Endpoint::Modex)
            .await
            .map_err(ModexError::Peer)?;

        let mut s = connect_peer(&addr).await?;
        s.write_all(&req).await?;
        let mut data = Vec::new();
        s.read_to_end(&mut data).await?;
        Ok(data)
    }

    async fn respond(
        mut c: net::TcpStream,
        request_fn: RequestFn,
    ) -> Result<(), ModexError<D::Error>> {
        let mut buf = [0; _];
        c.read_exact(&mut buf).await?;
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        let proc = Self::parse_proc(buf);
        let tx = Box::new(tx);

        // SAFETY: `request_fn` is PMIx_server_dmodex_request outside of tests.
        // `response` unwraps `cbdata` into oneshot::Sender<Vec<u8>>.
        let status =
            unsafe { (request_fn)(&proc, Some(response), Box::into_raw(tx) as *mut ffi::c_void) };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);

        let data = rx.await.expect("PMIx did not return modex response");
        c.write_all(&data).await?;
        Ok(())
    }

    pub async fn serve(
        self,
        events: mpsc::UnboundedReceiver<globals::DirectModexEvent>,
    ) -> Result<(), ModexError<D::Error>> {
        let requests = UnboundedReceiverStream::new(events).map(Ok).try_for_each(
            async |DirectModexEvent { proc, cb }| {
                let data = Self::request_data(self.discovery, proc).await?;
                cb.call(sys::PMIX_SUCCESS as sys::pmix_status_t, data);
                Ok(())
            },
        );
        let responses = TcpListenerStream::new(self.listener)
            .map_err(ModexError::from)
            .try_for_each(async |c| Self::respond(c, self.request_fn).await);

        let (requests, responses) = join(requests, responses).await;
        requests.and(responses)
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
        let discovery = DirectoryPeers::new(tmpdir.path(), nproc, 2);
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let requester = NetModex::new(addr, &discovery).await.unwrap();
        let responder = NetModex::with_request_fn(addr, &discovery, request_fn)
            .await
            .unwrap();
        discovery.register(&requester.addr()).unwrap();
        discovery.register(&responder.addr()).unwrap();

        let proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank: nproc as u32,
        };

        let (result_tx, result_rx) = oneshot::channel();
        let cb = globals::ModexCallback::test_callback(Box::new(move |data| {
            result_tx.send(Vec::from(data)).unwrap()
        }));

        let requester = {
            let (tx, rx) = mpsc::unbounded_channel();
            tx.send(globals::DirectModexEvent { proc, cb }).unwrap();
            pin!(requester.serve(rx))
        };
        let responder = pin!(responder.serve(mpsc::unbounded_channel().1));
        let Either::Left((Ok(data), _)) = select(result_rx, join(requester, responder)).await
        else {
            panic!("expected response");
        };
        assert_eq!(data, vec![1, 2, 3]);
    }
}
