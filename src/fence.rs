use std::{
    collections::HashMap,
    ffi,
    io::{self, Read, Write},
    net, slice,
    sync::Mutex,
};

use super::peer::DirPeerDiscovery;
use super::pmix::{globals, sys};

struct ActiveFence {
    data: Vec<u8>,
    acc: Vec<u8>,
    to_send: HashMap<u32, net::SocketAddr>,
    to_recv: usize,
    callback: globals::ModexCallback,
}

pub struct NetFence<'a> {
    discovery: &'a DirPeerDiscovery<'a>,
    listener: net::TcpListener,
    state: Mutex<Option<ActiveFence>>,
}

impl<'a> NetFence<'a> {
    pub fn new(addr: net::SocketAddr, discovery: &'a DirPeerDiscovery) -> Self {
        let state = Mutex::new(None);
        let listener = net::TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        Self {
            discovery,
            state,
            listener,
        }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub fn submit(
        &self,
        procs: Vec<sys::pmix_proc_t>,
        data: globals::CData,
        callback: globals::ModexCallback,
    ) {
        // TODO: Handle other fence scopes
        assert_eq!(procs.len(), 1);
        assert_eq!(procs[0].rank, sys::PMIX_RANK_WILDCARD);

        let (ptr, _) = data;
        let data = if !data.0.is_null() {
            let slice = unsafe { slice::from_raw_parts(data.0, data.1) };
            slice.to_vec()
        } else {
            Vec::new()
        };
        unsafe { libc::free(ptr as *mut ffi::c_void) }

        // We could exclude ourselves, but this works too.
        let peers = self.discovery.peers();
        let mut guard = self.state.lock().unwrap();
        assert!(guard.is_none());
        *guard = Some(ActiveFence {
            data,
            acc: Vec::new(),
            to_recv: peers.len(),
            to_send: peers,
            callback,
        });
    }

    pub fn handle_conns(&self) {
        let mut guard = self.state.lock().unwrap();
        {
            let Some(ref mut fence) = *guard else { return };

            while fence.to_recv > 0 {
                match self.listener.accept() {
                    Ok((mut c, _)) => {
                        c.read_to_end(&mut fence.acc).unwrap();
                        fence.to_recv -= 1;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("unexpected accept error: {e}"),
                };
            }

            fence.to_send.retain(|_, addr| {
                match net::TcpStream::connect(*addr) {
                    Ok(mut c) => {
                        c.write_all(&fence.data).unwrap();
                        false
                    }
                    // Peer is not listening yet
                    Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => true,
                    Err(e) => panic!("unexpected send error: {e}"),
                }
            });

            if fence.to_send.len() > 0 || fence.to_recv > 0 {
                return;
            }
        }

        let fence = guard.take().unwrap();
        let (Some(cbfunc), cbdata) = fence.callback else {
            return;
        };
        let data = Box::new(fence.acc);
        unsafe {
            cbfunc(
                sys::PMIX_SUCCESS as sys::pmix_status_t,
                data.as_ptr(),
                data.len(),
                cbdata,
                Some(globals::release_vec_u8),
                Box::into_raw(data) as *mut ffi::c_void,
            )
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::mpsc};

    use tempdir::TempDir;

    use super::*;

    type Cb = unsafe extern "C" fn(
        status: sys::pmix_status_t,
        data: *const ::std::os::raw::c_char,
        ndata: usize,
        cbdata: *mut ::std::os::raw::c_void,
        release_fn: sys::pmix_release_cbfunc_t,
        release_cbdata: *mut ::std::os::raw::c_void,
    );

    unsafe extern "C" fn cb(
        _status: sys::pmix_status_t,
        data: *const std::ffi::c_char,
        ndata: usize,
        cbdata: *mut std::ffi::c_void,
        release_fn: sys::pmix_release_cbfunc_t,
        release_cbdata: *mut std::ffi::c_void,
    ) {
        let cbdata = cbdata as *mut mpsc::Sender<Vec<u8>>;
        let tx = unsafe { Box::from_raw(cbdata) };
        let data = unsafe { slice::from_raw_parts(data, ndata) };
        tx.send(data.to_vec()).unwrap();
        if let Some(f) = release_fn {
            unsafe { f(release_cbdata) };
        }
    }

    fn alloc_data(data: &[u8]) -> (*mut u8, usize) {
        let size = data.len();
        let p = unsafe { libc::malloc(size) } as *mut u8;
        let slice = unsafe { slice::from_raw_parts_mut(p, size) };
        slice.copy_from_slice(data);
        (p, size)
    }

    #[test]
    fn test_fence() {
        let n = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirPeerDiscovery::new(tmpdir.path(), n);
        let fences = (0..n)
            .map(|_| {
                let addr = net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 0);
                NetFence::new(addr, &discovery)
            })
            .collect::<Vec<_>>();
        for (i, f) in fences.iter().enumerate() {
            discovery.register(&f.addr(), i as u32)
        }

        let proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank: sys::PMIX_RANK_WILDCARD,
        };
        let rxs = fences
            .iter()
            .enumerate()
            .map(|(i, fence)| {
                let data = alloc_data(&[i as u8]);
                let (tx, rx) = mpsc::channel::<Vec<u8>>();
                let tx = Box::new(tx);
                let callback = (Some(cb as Cb), Box::into_raw(tx) as *mut ffi::c_void);
                fence.submit(vec![proc], data, callback);
                fence.handle_conns(); // Test the case where not all members are ready
                rx
            })
            .collect::<Vec<_>>();

        let expected = (0..n as u8).collect::<HashSet<u8>>();
        for (fence, rx) in fences.iter().zip(rxs) {
            fence.handle_conns();
            assert_eq!(
                rx.try_recv().unwrap().into_iter().collect::<HashSet<_>>(),
                expected
            );
        }
    }
}
