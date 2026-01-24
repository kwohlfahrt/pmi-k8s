use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr;
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::sys;

static SERVER_LOCK: Mutex<()> = Mutex::new(());

pub struct Server {
    // I'm not sure what PMIx server functions are thread-safe, so just lock the
    // whole thing for now.
    _guard: MutexGuard<'static, ()>,
}

impl Server {
    pub fn init(infos: &[sys::pmix_info_t]) -> Result<Self, PoisonError<MutexGuard<'_, ()>>> {
        let server = Self {
            _guard: SERVER_LOCK.lock()?,
        };

        let mut module = sys::pmix_server_module_t {
            client_connected: None, // DEPRECATED
            client_finalized: None,
            abort: None,
            fence_nb: None,
            direct_modex: None,
            publish: None,
            lookup: None,
            unpublish: None,
            spawn: None,
            connect: None,
            disconnect: None,
            register_events: None,
            deregister_events: None,
            listener: None,
            /* v2x interfaces */
            notify_event: None,
            query: None,
            tool_connected: None, // DEPRECATED
            log: None,            // DEPRECATED
            allocate: None,
            job_control: None,
            monitor: None,
            /* v3x interfaces */
            get_credential: None,
            validate_credential: None,
            iof_pull: None,
            push_stdin: None,
            /* v4x interfaces */
            group: None,
            fabric: None,
            /* v6x interfaces */
            client_connected2: None,
            /* pending interfaces */
            session_control: None,
        };
        let status = unsafe {
            sys::PMIx_server_init(
                &mut module,
                infos.as_ptr() as *mut sys::pmix_info_t,
                infos.len(),
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as i32);

        Ok(server)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let status = unsafe { sys::PMIx_server_finalize() };
        assert_eq!(status, sys::PMIX_SUCCESS as i32)
    }
}

pub struct Namespace<'a> {
    nspace: sys::pmix_nspace_t,
    server: PhantomData<&'a Server>,
}

impl<'a> Namespace<'a> {
    pub fn register(_server: &'a mut Server, namespace: &CStr) -> Self {
        let mut infos: [sys::pmix_info_t; _] = [
            (sys::PMIX_SESSION_ID, 42 as u32).into(),
            (sys::PMIX_UNIV_SIZE, 2 as u32).into(),
            (sys::PMIX_JOBID, c"42.1").into(),
            (sys::PMIX_JOB_SIZE, 2 as u32).into(),
        ];

        let namespace = namespace.to_bytes_with_nul();
        let namespace = unsafe {
            std::slice::from_raw_parts(namespace.as_ptr() as *const libc::c_char, namespace.len())
        };
        let mut nspace: sys::pmix_nspace_t = [0; _];
        nspace[..namespace.len()].copy_from_slice(namespace);

        let status = unsafe {
            sys::PMIx_server_register_nspace(
                nspace.as_ptr(),
                1,
                infos.as_mut_ptr(),
                infos.len(),
                None,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(status, sys::PMIX_OPERATION_SUCCEEDED as i32);
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

    use super::super::is_initialized;
    use super::*;

    #[test]
    #[serial(server)]
    fn test_server_init() {
        assert!(!is_initialized());
        {
            let _s = Server::init(&mut []).unwrap();
            assert!(is_initialized());
        }
        assert!(!is_initialized());
    }
}
