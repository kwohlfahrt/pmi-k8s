use std::ffi::CStr;
use std::ffi::CString;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr;
use std::sync::mpsc;

use super::env;
use super::globals;
use super::sys;
use super::value::Rank;

pub struct Server<'a> {
    _tmpdir: &'a Path,
    _rx: mpsc::Receiver<globals::Event>,
    // I'm not sure what PMIx functions are thread-safe, so mark the server as
    // !Sync. Server::init enforces that only one is live at a time.
    _marker: globals::Unsync,
}

impl<'a> Server<'a> {
    pub fn init(tmpdir: &'a Path) -> Result<Self, globals::AlreadyInitialized> {
        let dirname = CString::new(tmpdir.as_os_str().as_encoded_bytes()).unwrap();
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
            _tmpdir: tmpdir,
            _rx: rx,
            _marker: globals::Unsync(PhantomData),
        })
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
    pub fn register(
        _server: &'a mut Server,
        namespace: &CStr,
        nlocalprocs: u32,
        nprocs: u32,
    ) -> Self {
        let namespace = namespace.to_bytes_with_nul();
        let namespace = unsafe {
            std::slice::from_raw_parts(namespace.as_ptr() as *const libc::c_char, namespace.len())
        };
        let mut nspace: sys::pmix_nspace_t = [0; _];
        nspace[..namespace.len()].copy_from_slice(namespace);

        let global_infos = [(sys::PMIX_JOB_SIZE, nprocs).into()];
        let proc_infos = (0..nlocalprocs)
            .map(|i| {
                [
                    (sys::PMIX_RANK, Rank(i)).into(),
                    (sys::PMIX_LOCAL_RANK, i as u16).into(),
                ]
            })
            .map(|infos| (sys::PMIX_PROC_INFO_ARRAY, infos.as_slice()).into());

        let mut infos = global_infos
            .into_iter()
            .chain(proc_infos)
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
    use serial_test::serial;
    use tempdir::TempDir;

    use super::super::is_initialized;
    use super::*;

    #[test]
    #[serial(server)]
    fn test_server_init() {
        assert!(!is_initialized());
        {
            let tempdir = TempDir::new("server").unwrap();
            let _s = Server::init(tempdir.path()).unwrap();
            assert!(is_initialized());
        }
        assert!(!is_initialized());
    }
}
