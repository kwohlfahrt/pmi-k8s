use std::{
    ffi,
    marker::PhantomData,
    sync::{RwLock, mpsc},
};
use thiserror::Error;

use super::sys;

pub enum Event {
    Fence {
        data: (*mut ffi::c_char, usize),
        cb: (sys::pmix_modex_cbfunc_t, *mut ffi::c_void),
    },
}

unsafe impl Send for Event {}

pub enum State {
    Client,
    Server(mpsc::Sender<Event>),
}

pub static PMIX_STATE: RwLock<Option<State>> = RwLock::new(None);

#[derive(Error, Debug)]
#[error("PMIx was already initialized")]
pub struct AlreadyInitialized();

pub struct Unsync(pub PhantomData<*const ()>);
unsafe impl Send for Unsync {}

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
    let guard = PMIX_STATE.read().unwrap();

    if let Some(State::Server(ref s)) = *guard {
        let cb = (cbfunc, cbdata);
        let data = (data, ndata);
        // mpsc::Sender only fails to send if the receiver is dropped. This only
        // happens in Server::drop, which also clears PMIX_STATE state.
        s.send(Event::Fence { data, cb }).unwrap();
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

pub fn server_module() -> sys::pmix_server_module_t {
    sys::pmix_server_module_t {
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
    }
}
