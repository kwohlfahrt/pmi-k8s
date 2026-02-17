use futures::future::select;
use std::cell::RefCell;
use std::convert::Infallible;
use std::ffi;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::pin;
use std::ptr;
use tokio::sync::mpsc;

use crate::ModexError;

use super::super::{fence, modex, peer};
use super::{
    u8_to_char, env, globals, sys,
    value::{PmixError, PmixStatus, Rank},
};

pub struct Server<'a, D: peer::PeerDiscovery> {
    fence: fence::NetFence<'a, D>,
    modex: modex::NetModex<'a, D>,
    rx: RefCell<mpsc::UnboundedReceiver<globals::Event>>,
    // I'm not sure what PMIx functions are thread-safe, so mark the server as
    // !Sync. Server::init enforces that only one is live at a time.
    _marker: globals::Unsync,
}

impl<'a, D: peer::PeerDiscovery> Server<'a, D> {
    pub fn init(
        fence: fence::NetFence<'a, D>,
        modex: modex::NetModex<'a, D>,
        dirname: &'a Path,
    ) -> Result<Self, globals::InitError> {
        #[allow(clippy::unwrap_used, reason = "File paths cannot contain NULL bytes")]
        let dirname = ffi::CString::new(dirname.as_os_str().as_encoded_bytes()).unwrap();
        let infos: [sys::pmix_info_t; _] = [
            (sys::PMIX_SERVER_TMPDIR, dirname.as_c_str()).into(),
            (sys::PMIX_SYSTEM_TMPDIR, dirname.as_c_str()).into(),
            (sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into(),
        ];
        let mut module = globals::server_module();

        #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
        let mut guard = globals::PMIX_STATE.write().unwrap();
        // TODO: Also check sys::PMIx_Initialized()
        if guard.is_some() {
            Err(globals::InitError::AlreadyInitialized)?;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        *guard = Some(globals::State::Server(tx));
        // SAFETY: global state accessed by the function pointers in `module` is
        // populated. `infos` is a pointer to an info array of length `ninfo`.
        PmixStatus(unsafe {
            sys::PMIx_server_init(&mut module, infos.as_ptr() as *mut _, infos.len())
        })
        .check()?;

        Ok(Self {
            fence,
            modex,
            rx: RefCell::new(rx),
            _marker: globals::Unsync(PhantomData),
        })
    }

    async fn handle_events(&self) -> Result<Infallible, ModexError<D::Error>> {
        loop {
            #[allow(
                clippy::unwrap_used,
                reason = "Sender is only dropped in Server::drop()"
            )]
            match self.rx.borrow_mut().recv().await.unwrap() {
                globals::Event::Fence { procs, data, cb } => {
                    self.fence.submit(&procs, data, cb).await?
                }
                globals::Event::DirectModex { proc, cb } => self.modex.request(proc, cb).await?,
            }
        }
    }

    pub async fn run(&self) -> Result<Infallible, ModexError<D::Error>> {
        let events = pin!(self.handle_events());
        let modex = pin!(self.modex.serve());
        select(events, modex).await.factor_first().0
    }
}

impl<'a, D: peer::PeerDiscovery> Drop for Server<'a, D> {
    fn drop(&mut self) {
        // SAFETY: We call server finalize before dropping the global state, to
        // ensure libpmix does not call the global functions without the
        // required state set up. We must have called `PMIx_server_init` before,
        // to acquire the server object being dropped.
        let status = unsafe { sys::PMIx_server_finalize() };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);

        #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
        let mut guard = globals::PMIX_STATE.write().unwrap();
        drop(guard.take());
    }
}

pub struct Namespace<'a, D: peer::PeerDiscovery> {
    nspace: sys::pmix_nspace_t,
    server: PhantomData<&'a Server<'a, D>>,
}

impl<'a, D: peer::PeerDiscovery> Namespace<'a, D> {
    // TODO: This should be a method on Server
    pub fn register(
        _server: &'a Server<D>,
        namespace: &ffi::CStr,
        hostnames: &[&ffi::CStr],
        nlocalprocs: u16,
    ) -> Result<Self, PmixError> {
        let namespace = namespace.to_bytes_with_nul();
        let mut nspace: sys::pmix_nspace_t = [0; _];
        nspace[..namespace.len()].copy_from_slice(u8_to_char(namespace));

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
                        (sys::PMIX_LOCAL_RANK, i).into(),
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

        // SAFETY: No significant safety concerns.
        PmixStatus(unsafe {
            sys::PMIx_server_register_nspace(
                nspace.as_ptr(),
                nlocalprocs as i32,
                infos.as_mut_ptr(),
                infos.len(),
                None,
                std::ptr::null_mut(),
            )
        })
        .check()?;
        Ok(Self {
            nspace,
            server: PhantomData,
        })
    }
}

impl<'a, D: peer::PeerDiscovery> Drop for Namespace<'a, D> {
    fn drop(&mut self) {
        // SAFETY: We must have called `PMIx_server_register_nspace` to acquire
        // the namespace object being dropped.
        unsafe {
            sys::PMIx_server_deregister_nspace(self.nspace.as_ptr(), None, ptr::null_mut());
        }
    }
}

pub struct Client<'a, D: peer::PeerDiscovery> {
    proc: sys::pmix_proc_t,
    namespace: PhantomData<&'a Namespace<'a, D>>,
}

impl<'a, D: peer::PeerDiscovery> Client<'a, D> {
    pub fn register(namespace: &'a Namespace<D>, rank: u32) -> Result<Self, PmixError> {
        let uid = nix::unistd::geteuid();
        let gid = nix::unistd::getegid();

        let proc = sys::pmix_proc_t {
            nspace: namespace.nspace,
            rank,
        };

        // SAFETY: No significant safety concerns.
        PmixStatus(unsafe {
            sys::PMIx_server_register_client(
                &proc,
                uid.as_raw(),
                gid.as_raw(),
                ptr::null_mut(),
                None,
                ptr::null_mut(),
            )
        })
        .check()?;
        Ok(Client {
            proc,
            namespace: PhantomData,
        })
    }

    pub fn envs(&self) -> Result<env::EnvVars, PmixError> {
        let mut env = ptr::null_mut();
        // SAFETY: `self.proc` is an initialized client, and `env` is a pointer
        // to an empty `argv`-style array.
        PmixStatus(unsafe { sys::PMIx_server_setup_fork(&self.proc, &mut env) }).check()?;
        // SAFETY: The env array was created by `PMIx_server_setup_fork`, so is
        // in the correct format (`argv`-style) for `EnvVars`.
        let env = unsafe { env::EnvVars::from_ptr(env) };
        Ok(env)
    }
}

impl<'a, D: peer::PeerDiscovery> Drop for Client<'a, D> {
    fn drop(&mut self) {
        // SAFETY: We must have called `PMIx_server_register_client` to acquire
        // the client object being dropped.
        unsafe {
            sys::PMIx_server_deregister_client(&self.proc, None, ptr::null_mut());
        }
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use std::net;

    use serial_test::serial;
    use tempdir::TempDir;

    use super::super::super::peer::DirectoryPeers;
    use super::super::is_initialized;
    use super::*;

    #[tokio::test]
    #[serial(server)]
    async fn test_server_init() {
        assert!(!is_initialized());
        {
            let tempdir = TempDir::new("peers").unwrap();
            let discovery = DirectoryPeers::new(tempdir.path(), 1);
            let fence = fence::NetFence::new(
                net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 0),
                &discovery,
            )
            .await
            .unwrap();
            let modex = modex::NetModex::new(
                net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 0),
                &discovery,
                1,
            )
            .await
            .unwrap();

            let tempdir = TempDir::new("server").unwrap();
            let _s = Server::init(fence, modex, tempdir.path()).unwrap();
            assert!(is_initialized());
        }
        assert!(!is_initialized());
    }
}
