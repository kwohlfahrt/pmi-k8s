use futures::future::select;
use std::ffi;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::pin;
use std::ptr;
use std::str::FromStr;
use tokio::sync::mpsc;

use crate::ModexError;
use crate::peer::PeerDiscovery;

use super::super::{fence, modex};
use super::{
    env, globals,
    info::{self, Key},
    sys, u8_to_char,
    value::{PmixError, PmixStatus},
};

pub struct ServerEvents<'a> {
    fence_rx: mpsc::UnboundedReceiver<globals::FenceEvent>,
    modex_rx: mpsc::UnboundedReceiver<globals::DirectModexEvent>,
    _server: &'a PhantomData<Server<'a>>,
}

impl<'a> ServerEvents<'a> {
    pub async fn run<D: PeerDiscovery>(
        self,
        fence: fence::NetFence<'a, D>,
        modex: modex::NetModex<'a, D>,
    ) -> Result<(), ModexError<D::Error>> {
        let fence = pin!(fence.serve(self.fence_rx));
        let modex = pin!(modex.serve(self.modex_rx));
        select(fence, modex).await.factor_first().0
    }
}

pub struct Server<'a> {
    _dir: &'a PhantomData<Path>,
}

impl<'a> Server<'a> {
    pub fn init(dirname: &'a Path) -> Result<(Self, ServerEvents<'a>), globals::InitError> {
        #[allow(clippy::unwrap_used, reason = "File paths cannot contain NULL bytes")]
        let dirname = ffi::CString::new(dirname.as_os_str().as_encoded_bytes()).unwrap();
        let infos: [sys::pmix_info_t; _] = [
            info::ServerTmpdir::info(dirname.as_c_str()),
            info::SystemTmpdir::info(dirname.as_c_str()),
            info::ServerSystemSupport::info(&true),
        ];
        let mut module = globals::server_module();

        #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
        let mut guard = globals::PMIX_STATE.write().unwrap();
        // TODO: Also check sys::PMIx_Initialized()
        if guard.is_some() {
            Err(globals::InitError::AlreadyInitialized)?;
        }
        let (fence_tx, fence_rx) = mpsc::unbounded_channel();
        let (modex_tx, modex_rx) = mpsc::unbounded_channel();
        *guard = Some(globals::State::Server { fence_tx, modex_tx });
        // SAFETY: global state accessed by the function pointers in `module` is
        // populated. `infos` is a pointer to an info array of length `ninfo`.
        PmixStatus(unsafe {
            sys::PMIx_server_init(&mut module, infos.as_ptr() as *mut _, infos.len())
        })
        .check()?;

        Ok((
            Self { _dir: &PhantomData },
            ServerEvents {
                fence_rx,
                modex_rx,
                _server: &PhantomData,
            },
        ))
    }
}

impl<'a> Drop for Server<'a> {
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

pub struct Namespace<'a> {
    nspace: sys::pmix_nspace_t,
    server: PhantomData<&'a Server<'a>>,
}

impl<'a> Namespace<'a> {
    // TODO: This should be a method on Server
    pub fn register(
        _server: &'a Server,
        namespace: &ffi::CStr,
        hostnames: &[String],
        nlocalprocs: u16,
    ) -> Result<Self, PmixError> {
        let namespace = namespace.to_bytes_with_nul();
        let mut nspace: sys::pmix_nspace_t = [0; _];
        nspace[..namespace.len()].copy_from_slice(u8_to_char(namespace));

        let nnodes = hostnames.len() as u32;
        let node_map = hostnames.join(",");
        let node_map = ffi::CString::from_str(&node_map).expect("invalid node map generated");

        let proc_map = (0..nnodes)
            .map(|node_rank| {
                (0..nlocalprocs)
                    .map(move |i| {
                        let rank = (nlocalprocs as u32 * node_rank) + i as u32;
                        rank.to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect::<Vec<_>>()
            .join(";");
        let proc_map = ffi::CString::from_str(&proc_map).expect("invalid proc map generated");

        let mut infos = [
            info::JobSize::info(&(nnodes * nlocalprocs as u32)),
            info::ProcMap::info(&proc_map),
            info::NodeMap::info(&node_map),
        ];

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

impl<'a> Drop for Namespace<'a> {
    fn drop(&mut self) {
        // SAFETY: We must have called `PMIx_server_register_nspace` to acquire
        // the namespace object being dropped.
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
    pub fn register(namespace: &'a Namespace, rank: u32) -> Result<Self, PmixError> {
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

impl<'a> Drop for Client<'a> {
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
    use serial_test::serial;
    use tempdir::TempDir;

    use super::super::is_initialized;
    use super::*;

    #[tokio::test]
    #[serial(server)]
    async fn test_server_init() {
        assert!(!is_initialized());
        {
            let tempdir = TempDir::new("server").unwrap();
            let _s = Server::init(tempdir.path()).unwrap();
            assert!(is_initialized());
        }
        assert!(!is_initialized());
    }
}
