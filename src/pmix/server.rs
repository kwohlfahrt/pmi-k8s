use std::ffi;
use std::marker::PhantomData;
use std::ptr;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use tempdir::TempDir;

use super::super::{fence, modex};
use super::{env, globals, sys, value::Rank};

pub struct Server<'a> {
    fence: fence::NetFence<'a>,
    modex: modex::NetModex<'a>,
    rx: mpsc::Receiver<globals::Event>,
    _tmpdir: TempDir,
    // I'm not sure what PMIx functions are thread-safe, so mark the server as
    // !Sync. Server::init enforces that only one is live at a time.
    _marker: globals::Unsync,
}

impl<'a> Server<'a> {
    pub fn init(
        fence: fence::NetFence<'a>,
        modex: modex::NetModex<'a>,
    ) -> Result<Self, globals::AlreadyInitialized> {
        let tmpdir = TempDir::new("pmix-server").unwrap();
        let dirname = ffi::CString::new(tmpdir.path().as_os_str().as_encoded_bytes()).unwrap();
        let infos: [sys::pmix_info_t; _] = [
            (sys::PMIX_SERVER_TMPDIR, dirname.as_c_str()).into(),
            (sys::PMIX_SYSTEM_TMPDIR, dirname.as_c_str()).into(),
            (sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into(),
        ];
        let mut module = globals::server_module();

        let mut guard = globals::PMIX_STATE.write().unwrap();
        // TODO: Also check sys::PMIx_Initialized()
        if guard.is_some() {
            return Err(globals::AlreadyInitialized());
        }
        let (tx, rx) = mpsc::channel();
        let status =
            unsafe { sys::PMIx_server_init(&mut module, infos.as_ptr() as *mut _, infos.len()) };
        // FIXME: Don't poison the lock on failure
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        *guard = Some(globals::State::Server(tx));

        Ok(Self {
            fence,
            modex,
            rx,
            _tmpdir: tmpdir,
            _marker: globals::Unsync(PhantomData),
        })
    }

    pub fn handle_event(&self, timeout: Duration) {
        self.fence.handle_conns();
        self.modex.handle_conns();
        // mpsc::Receiver only fails if all senders are dropped, which only
        // happens during our own drop.
        match self.rx.recv_timeout(timeout) {
            Ok(globals::Event::Fence { procs, data, cb }) => self.fence.submit(procs, data, cb),
            Ok(globals::Event::DirectModex { proc, cb }) => self.modex.request(proc, cb),
            Ok(globals::Event::DirectModexResponse { proc, data }) => {
                self.modex.send_response(proc, data)
            }
            Err(RecvTimeoutError::Timeout) => (),
            _ => unimplemented!(),
        }
    }
}

impl<'a> Drop for Server<'a> {
    fn drop(&mut self) {
        let mut guard = globals::PMIX_STATE.write().unwrap();
        drop(guard.take());
        let status = unsafe { sys::PMIx_server_finalize() };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t)
    }
}

pub struct Namespace<'a> {
    nspace: sys::pmix_nspace_t,
    server: PhantomData<&'a Server<'a>>,
}

impl<'a> Namespace<'a> {
    // TODO: This should be a method on Server
    pub fn register(
        _server: &'a Server,
        namespace: &ffi::CStr,
        hostnames: &[&ffi::CStr],
        nlocalprocs: u16,
    ) -> Self {
        let namespace = namespace.to_bytes_with_nul();
        let namespace = unsafe {
            std::slice::from_raw_parts(namespace.as_ptr() as *const libc::c_char, namespace.len())
        };
        let mut nspace: sys::pmix_nspace_t = [0; _];
        nspace[..namespace.len()].copy_from_slice(namespace);

        let nnodes = hostnames.len() as u32;

        let node_infos = hostnames
            .iter()
            .enumerate()
            .map(|(node_rank, hostname)| {
                [
                    (sys::PMIX_HOSTNAME, *hostname).into(),
                    (sys::PMIX_NODEID, node_rank).into(),
                ]
            })
            .map(|infos| (sys::PMIX_NODE_INFO_ARRAY, infos.as_slice()).into());
        let global_infos = [(sys::PMIX_JOB_SIZE, nnodes * nlocalprocs as u32).into()];

        let proc_infos = (0..nnodes).flat_map(|node_rank| {
            (0..nlocalprocs)
                .map(move |i| {
                    let rank = Rank((nlocalprocs as u32 * node_rank) + i as u32);
                    [
                        (sys::PMIX_RANK, rank).into(),
                        (sys::PMIX_LOCAL_RANK, i as u16).into(),
                        (sys::PMIX_NODEID, node_rank).into(),
                    ]
                })
                .map(|infos| (sys::PMIX_PROC_INFO_ARRAY, infos.as_slice()).into())
        });

        let mut infos = global_infos
            .into_iter()
            .chain(proc_infos)
            .chain(node_infos)
            .collect::<Vec<_>>();

        let status = unsafe {
            sys::PMIx_server_register_nspace(
                nspace.as_ptr(),
                nlocalprocs as i32,
                infos.as_mut_ptr(),
                infos.len(),
                None,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(status, sys::PMIX_OPERATION_SUCCEEDED as sys::pmix_status_t);
        Self {
            nspace,
            server: PhantomData,
        }
    }
}

impl<'a> Drop for Namespace<'a> {
    fn drop(&mut self) {
        unsafe {
            sys::PMIx_server_deregister_nspace(self.nspace.as_ptr(), None, ptr::null_mut());
        }
    }
}

pub struct Client<'a> {
    proc: sys::pmix_proc_t,
    namespace: PhantomData<&'a Namespace<'a>>,
}

impl<'a> Client<'a> {
    pub fn register(namespace: &'a Namespace, rank: u32) -> Self {
        let uid = nix::unistd::geteuid();
        let gid = nix::unistd::getegid();

        let proc = sys::pmix_proc_t {
            nspace: namespace.nspace,
            rank,
        };

        let status = unsafe {
            sys::PMIx_server_register_client(
                &proc,
                uid.as_raw(),
                gid.as_raw(),
                ptr::null_mut(),
                None,
                ptr::null_mut(),
            )
        };
        assert_eq!(status, sys::PMIX_OPERATION_SUCCEEDED as i32);
        Client {
            proc,
            namespace: PhantomData,
        }
    }

    pub fn envs(&self) -> env::EnvVars {
        let mut env = ptr::null_mut();
        let status = unsafe { sys::PMIx_server_setup_fork(&self.proc, &mut env) };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        unsafe { env::EnvVars::from_ptr(env) }
    }
}

impl<'a> Drop for Client<'a> {
    fn drop(&mut self) {
        unsafe {
            sys::PMIx_server_deregister_client(&self.proc, None, ptr::null_mut());
        }
    }
}

#[cfg(test)]
mod test {
    use std::net;

    use serial_test::serial;
    use tempdir::TempDir;

    use super::super::super::peer::DirPeerDiscovery;
    use super::super::is_initialized;
    use super::*;

    #[test]
    #[serial(server)]
    fn test_server_init() {
        assert!(!is_initialized());
        {
            let tempdir = TempDir::new("server").unwrap();
            let discovery = DirPeerDiscovery::new(tempdir.path(), 1);
            let fence = fence::NetFence::new(
                net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 0),
                &discovery,
            );
            let modex = modex::NetModex::new(
                net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 0),
                &discovery,
                1,
            );
            let _s = Server::init(fence, modex).unwrap();
            assert!(is_initialized());
        }
        assert!(!is_initialized());
    }
}
