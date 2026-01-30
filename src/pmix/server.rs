use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr;
use std::sync::mpsc;

use super::env;
use super::globals;
use super::sys;
use super::value::Rank;

pub struct Server {
    _rx: mpsc::Receiver<globals::Event>,
    // I'm not sure what PMIx functions are thread-safe, so mark the server as
    // !Sync. Server::init enforces that only one is live at a time.
    _marker: globals::Unsync,
}

/* For callbacks, one must either:
 * 1. Return PMIX_OPERATION_SUCCEEDED
 * 2. Call return PMIX_SUCCESS, then call cbfunc(PMIX_SUCCESS, cbdata)
 */

unsafe extern "C" fn client_connected(
    _proc: *const sys::pmix_proc_t,
    _server_object: *mut std::ffi::c_void,
    _info: *mut sys::pmix_info_t,
    ninfo: usize,
    _cbfunc: sys::pmix_op_cbfunc_t,
    _cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    println!("client_connected2 called, ninfo: {}", ninfo);
    sys::PMIX_OPERATION_SUCCEEDED as sys::pmix_status_t
}

unsafe extern "C" fn fence_nb(
    _procs: *const sys::pmix_proc_t,
    nprocs: usize,
    info: *const sys::pmix_info_t,
    ninfo: usize,
    data: *mut std::ffi::c_char,
    ndata: usize,
    cbfunc: sys::pmix_modex_cbfunc_t,
    cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    let info = unsafe { std::slice::from_raw_parts(info, ninfo) };
    let ninfo_reqd = info
        .iter()
        .filter(|i| {
            (i.flags & sys::PMIX_INFO_REQD != 0) && (i.flags & sys::PMIX_INFO_REQD_PROCESSED == 0)
        })
        .count();
    println!(
        "fence_nb called: nprocs={} ninfo={} ({}) ndata={}",
        nprocs, ninfo, ninfo_reqd, ndata
    );
    if ninfo_reqd > 0 {
        return sys::PMIX_ERR_NOT_SUPPORTED;
    };
    let guard = globals::PMIX_STATE.read().unwrap();

    if let Some(globals::State::Server(ref s)) = *guard {
        let cb = (cbfunc, cbdata);
        let data = (data, ndata);
        // mpsc::Sender only fails to send if the receiver is dropped. This only
        // happens in Server::drop, which also clears PMIX_STATE state.
        s.send(globals::Event::Fence { data, cb }).unwrap();
        sys::PMIX_SUCCESS as sys::pmix_status_t
    } else {
        sys::PMIX_ERR_INIT as sys::pmix_status_t
    }
}

unsafe extern "C" fn direct_modex(
    _proc_: *const sys::pmix_proc_t,
    _info: *const sys::pmix_info_t,
    _ninfo: usize,
    _cbfunc: sys::pmix_modex_cbfunc_t,
    _cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    println!("direct_modex called");
    sys::PMIX_ERR_NOT_SUPPORTED as sys::pmix_status_t
}

unsafe extern "C" fn publish(
    _proc_: *const sys::pmix_proc_t,
    _info: *const sys::pmix_info_t,
    _ninfo: usize,
    _cbfunc: sys::pmix_op_cbfunc_t,
    _cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    println!("publish called");
    sys::PMIX_ERR_NOT_SUPPORTED as sys::pmix_status_t
}

unsafe extern "C" fn lookup(
    _proc_: *const sys::pmix_proc_t,
    _keys: *mut *mut std::ffi::c_char,
    _info: *const sys::pmix_info_t,
    _ninfo: usize,
    _cbfunc: sys::pmix_lookup_cbfunc_t,
    _cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    println!("lookup called");
    sys::PMIX_ERR_NOT_SUPPORTED as sys::pmix_status_t
}

unsafe extern "C" fn query(
    _proct: *mut sys::pmix_proc_t,
    _queries: *mut sys::pmix_query_t,
    _nqueries: usize,
    _cbfunc: sys::pmix_info_cbfunc_t,
    _cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    println!("query called");
    sys::PMIX_ERR_NOT_SUPPORTED as sys::pmix_status_t
}

impl Server {
    pub fn init(infos: &[sys::pmix_info_t]) -> Result<Self, globals::AlreadyInitialized> {
        let mut module = sys::pmix_server_module_t {
            client_connected: None, // DEPRECATED
            client_finalized: None,
            abort: None,
            fence_nb: Some(fence_nb),
            direct_modex: Some(direct_modex),
            publish: Some(publish),
            lookup: Some(lookup),
            unpublish: None,
            spawn: None,
            connect: None,
            disconnect: None,
            register_events: None,
            deregister_events: None,
            listener: None,
            /* v2x interfaces */
            notify_event: None,
            query: Some(query),
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
            client_connected2: Some(client_connected),
            /* pending interfaces */
            session_control: None,
        };

        let mut guard = globals::PMIX_STATE.write().unwrap();
        // TODO: Also check sys::PMIx_Initialized()
        if guard.is_some() {
            return Err(globals::AlreadyInitialized());
        }
        let (tx, rx) = mpsc::channel();
        let status = unsafe {
            sys::PMIx_server_init(
                &mut module,
                infos.as_ptr() as *mut sys::pmix_info_t,
                infos.len(),
            )
        };
        // FIXME: Don't poison the lock on failure
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        *guard = Some(globals::State::Server(tx));

        Ok(Self {
            _rx: rx,
            _marker: globals::Unsync(PhantomData),
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let mut guard = globals::PMIX_STATE.write().unwrap();
        drop(guard.take());
        let status = unsafe { sys::PMIx_server_finalize() };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t)
    }
}

pub struct Namespace<'a> {
    nspace: sys::pmix_nspace_t,
    server: PhantomData<&'a Server>,
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
