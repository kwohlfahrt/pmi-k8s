use std::ffi;
use std::pin::pin;
use std::{mem, net::SocketAddr};

use futures::FutureExt;
use futures::{StreamExt, future::select};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net,
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
use tracing::warn;

use crate::net::connect_peer;
use crate::pmix::{PmixError, PmixStatus};
use crate::{
    ModexError,
    peer::{Endpoint, PeerDiscovery},
    pmix::{
        char_to_u8,
        globals::{self, DirectModexEvent},
        slice_from_raw_parts, sys, u8_to_char,
    },
};

type ModexResponse = Result<Vec<u8>, PmixError>;

unsafe extern "C" fn response(
    status: sys::pmix_status_t,
    data: *mut std::ffi::c_char,
    sz: usize,
    cbdata: *mut std::ffi::c_void,
) {
    let data = PmixStatus(status).check().map(|()| {
        // SAFETY: Data is owned by PMIx and free'd after this function, so we must
        // copy before returning.
        let data = unsafe { slice_from_raw_parts(data, sz) };
        char_to_u8(data).to_vec()
    });

    // SAFETY: We created `cbdata`` in `NetModex::respond`
    let tx = *unsafe { Box::from_raw(cbdata as *mut oneshot::Sender<ModexResponse>) };

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
        let mut status = [0; _];
        s.read_exact(&mut status).await?;
        PmixStatus(sys::pmix_status_t::from_be_bytes(status)).check()?;

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
        let (tx, rx) = oneshot::channel::<ModexResponse>();
        let proc = Self::parse_proc(buf);
        let tx = Box::into_raw(Box::new(tx));

        // SAFETY: `request_fn` is PMIx_server_dmodex_request outside of tests.
        // `response` unwraps `cbdata` into oneshot::Sender<Vec<u8>>.
        let status = unsafe { (request_fn)(&proc, Some(response), tx as *mut ffi::c_void) };
        PmixStatus(status).check().map_err(|err| {
            // SAFETY: If `PMIx_server_dmodex_request` returns a non-success
            // code, the callback won't be called, so we must reclaim it.
            let tx = unsafe { Box::from_raw(tx) };
            drop(tx);
            ModexError::Server(err)
        })?;

        match rx.await.expect("modex response never sent") {
            Ok(data) => {
                let code = sys::PMIX_SUCCESS as sys::pmix_status_t;
                c.write_all(&code.to_be_bytes()).await?;
                Ok(c.write_all(&data).await?)
            }
            Err(err @ PmixError(code)) => {
                c.write_all(&code.to_be_bytes()).await?;
                Err(ModexError::Server(err))
            }
        }
    }

    pub async fn serve(
        self,
        events: mpsc::UnboundedReceiver<globals::DirectModexEvent>,
    ) -> Result<(), ModexError<D::Error>> {
        let requests = UnboundedReceiverStream::new(events)
            .map(|DirectModexEvent { proc, cb }| {
                Self::request_data(self.discovery, proc).map(|r| (cb, r))
            })
            .buffer_unordered(8)
            .for_each(async |(cb, result)| match result {
                Ok(data) => cb.call(sys::PMIX_SUCCESS as sys::pmix_status_t, data),
                Err(err) => {
                    warn!(%err, "modex request");
                    cb.call(sys::PMIX_ERROR as sys::pmix_status_t, Vec::new());
                }
            });
        let responses =
            TcpListenerStream::new(self.listener).for_each_concurrent(8, async |c| match c {
                Ok(c) => Self::respond(c, self.request_fn)
                    .await
                    .unwrap_or_else(|err| warn!(%err, "modex response")),
                Err(err) => warn!(%err, "modex accept"),
            });

        let ((), _) = select(pin!(requests), pin!(responses)).await.factor_first();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used, clippy::panic, clippy::undocumented_unsafe_blocks)]
    use crate::peer::DirectoryPeers;
    use std::{net::Ipv4Addr, pin::pin};

    use super::*;
    use futures::{
        TryFutureExt, TryStreamExt,
        future::{Either, join, select},
    };
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

    type TestError<'a> = ModexError<<DirectoryPeers<'a> as PeerDiscovery>::Error>;
    async fn create_modex<'a>(
        discovery: &'a DirectoryPeers<'a>,
    ) -> (
        impl Future<Output = Result<(), TestError<'a>>>,
        mpsc::UnboundedSender<globals::DirectModexEvent>,
    ) {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let modex = NetModex::with_request_fn(addr, discovery, request_fn)
            .await
            .unwrap();
        discovery.register(&modex.addr()).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        (modex.serve(rx), tx)
    }

    fn create_event(
        proc: sys::pmix_proc_t,
    ) -> (
        globals::DirectModexEvent,
        oneshot::Receiver<(sys::pmix_status_t, Vec<u8>)>,
    ) {
        let (tx, rx) = oneshot::channel();
        let cb = globals::ModexCallback::test_callback(Box::new(move |status, data| {
            tx.send((status, Vec::from(data))).unwrap()
        }));
        (globals::DirectModexEvent { proc, cb }, rx)
    }

    #[tokio::test]
    async fn test_modex() {
        let nproc = 4;

        let tmpdir = TempDir::new("modex-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), nproc, 2);
        let (requester, tx) = create_modex(&discovery).await;
        let (responder, _tx) = create_modex(&discovery).await;

        let proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank: nproc as u32,
        };

        let (event, rx) = create_event(proc);
        tx.send(event).unwrap();
        let Either::Left((Ok((status, data)), _)) =
            select(rx, join(pin!(requester), pin!(responder))).await
        else {
            panic!("expected response");
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        assert_eq!(data, vec![1, 2, 3]);
    }

    async fn create_bad_modex<'a>(
        discovery: &'a DirectoryPeers<'a>,
    ) -> impl Future<Output = Result<(), TestError<'a>>> {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let l = net::TcpListener::bind(addr).await.unwrap();
        let addr = l.local_addr().unwrap();
        discovery.register(&addr).unwrap();

        TcpListenerStream::new(l)
            .try_for_each(async |s| {
                drop(s);
                Ok(())
            })
            .map_err(ModexError::from)
    }

    #[tokio::test]
    async fn test_modex_err() {
        let nproc = 4;

        let tmpdir = TempDir::new("modex-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), nproc, 2);
        let (requester, tx) = create_modex(&discovery).await;
        let responder = create_bad_modex(&discovery).await;

        let proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank: nproc as u32,
        };

        let (event, rx) = create_event(proc);
        tx.send(event).unwrap();
        let Either::Left((Ok(result), _)) =
            select(rx, join(pin!(requester), pin!(responder))).await
        else {
            panic!("expected response");
        };
        assert_eq!(result, (sys::PMIX_ERROR, vec![]));
    }
}
