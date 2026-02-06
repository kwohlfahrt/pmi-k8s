use std::{
    collections::HashMap,
    ffi::{self},
    io::{self, Read, Write},
    mem::{self},
    net, slice,
    sync::Mutex,
};

use crate::{peer::DirPeerDiscovery, pmix::globals::ModexCallback};

use super::pmix::{globals, sys};

unsafe extern "C" fn response(
    status: sys::pmix_status_t,
    data: *mut std::ffi::c_char,
    sz: usize,
    cbdata: *mut std::ffi::c_void,
) {
    assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
    let data = if !data.is_null() {
        unsafe { slice::from_raw_parts(data, sz) }
    } else {
        &[]
    };
    let data = data.to_vec();
    let proc = *unsafe { Box::from_raw(cbdata as *mut sys::pmix_proc_t) };

    let guard = globals::PMIX_STATE.read().unwrap();
    if let Some(globals::State::Server(ref s)) = *guard {
        s.send(globals::Event::DirectModexResponse { proc, data })
            .unwrap();
    }
}

struct ActiveModex {
    to_send: HashMap<u32, net::TcpStream>,
    to_recv: HashMap<u32, (net::TcpStream, Vec<u8>, ModexCallback)>,
}

pub struct NetModex<'a> {
    peers: &'a DirPeerDiscovery<'a>,
    listener: net::TcpListener,
    nproc: u16,
    active: Mutex<ActiveModex>,
}

impl<'a> NetModex<'a> {
    pub fn new(addr: net::SocketAddr, peers: &'a DirPeerDiscovery, nproc: u16) -> Self {
        let listener = net::TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();

        Self {
            listener,
            peers,
            nproc,
            active: Mutex::new(ActiveModex {
                to_send: HashMap::new(),
                to_recv: HashMap::new(),
            }),
        }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub fn request(&self, proc: sys::pmix_proc_t, cb: globals::ModexCallback) {
        assert!(proc.rank <= sys::PMIX_RANK_VALID);
        let node_rank = proc.rank / self.nproc as u32;

        let peer = self.peers.peers()[&node_rank];
        let mut s = net::TcpStream::connect(peer).unwrap();
        s.write_all(&proc.rank.to_be_bytes()).unwrap();
        s.write_all(&proc.nspace).unwrap();
        s.set_nonblocking(true).unwrap();
        let mut guard = self.active.lock().unwrap();
        guard.to_recv.insert(proc.rank, (s, Vec::new(), cb));
    }

    fn handle_request(&self, mut s: net::TcpStream) {
        let mut rank = [0; mem::size_of::<u32>()];
        s.read_exact(&mut rank).unwrap();
        let rank = u32::from_be_bytes(rank);
        let mut nspace = [0; mem::size_of::<sys::pmix_nspace_t>()];
        s.read_exact(&mut nspace).unwrap();
        let proc = sys::pmix_proc_t { rank, nspace };

        let dst = Box::new(proc);

        let status = unsafe {
            sys::PMIx_server_dmodex_request(
                &proc,
                Some(response),
                Box::into_raw(dst) as *mut ffi::c_void,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);

        let mut guard = self.active.lock().unwrap();
        guard.to_send.insert(proc.rank, s);
    }

    fn receive_response(&self, s: &mut net::TcpStream, acc: &mut Vec<u8>) -> bool {
        match s.read_to_end(acc) {
            Ok(_) => true,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => false,
            Err(e) => panic!("unexpected read error: {e}"),
        }
    }

    pub fn handle_conns(&self) {
        loop {
            match self.listener.accept() {
                Ok((c, _)) => self.handle_request(c),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("unexpected accept error: {e}"),
            };
        }

        let mut guard = self.active.lock().unwrap();
        let done = guard
            .to_recv
            .extract_if(|_, (s, acc, _)| self.receive_response(s, acc));

        for (_, (_, acc, cb)) in done {
            let data = Box::new(acc);
            if let (Some(cbfunc), cbdata) = cb {
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
    }

    pub fn send_response(&self, proc: sys::pmix_proc_t, data: Vec<u8>) {
        let mut guard = self.active.lock().unwrap();
        let mut c = guard.to_send.remove(&proc.rank).unwrap();
        c.write_all(&data).unwrap();
    }
}
