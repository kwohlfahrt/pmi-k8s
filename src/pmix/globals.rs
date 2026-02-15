use std::{ffi, marker::PhantomData, slice, sync::RwLock};
use tokio::sync::mpsc;

use super::{slice_from_raw_parts, sys, value::PmixError};

pub type ModexCallback = (sys::pmix_modex_cbfunc_t, *mut ffi::c_void);
pub type CData = (*mut ffi::c_char, usize);

pub enum Event {
    Fence {
        procs: Vec<sys::pmix_proc_t>,
        data: CData,
        cb: ModexCallback,
    },
    DirectModex {
        proc: sys::pmix_proc_t,
        cb: (sys::pmix_modex_cbfunc_t, *mut ffi::c_void),
    },
}

unsafe impl Send for Event {}

pub enum State {
    Client,
    Server(mpsc::UnboundedSender<Event>),
}

pub static PMIX_STATE: RwLock<Option<State>> = RwLock::new(None);

#[derive(thiserror::Error, Debug)]
pub enum InitError {
    #[error("PMIx operation returned error code {}", 0.0)]
    PmixError(#[from] PmixError),
    #[error("PMIx global state was already initialized")]
    AlreadyInitialized,
}

pub struct Unsync(pub PhantomData<*const ()>);
// SAFETY: This is a marker type, for `Send + !Sync`
unsafe impl Send for Unsync {}

/// # Safety
///
/// `cbdata` must be a pointer created from `Box<Vec<u8>>::into_raw()`
pub unsafe extern "C" fn release_vec_u8(cbdata: *mut ffi::c_void) {
    // SAFETY: The inverse of the creation of `cbdata`
    let data = unsafe { Box::from_raw(cbdata as *mut Vec<u8>) };
    drop(data)
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
    procs: *const sys::pmix_proc_t,
    nprocs: usize,
    info: *const sys::pmix_info_t,
    ninfo: usize,
    data: *mut std::ffi::c_char,
    ndata: usize,
    cbfunc: sys::pmix_modex_cbfunc_t,
    cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    // SAFETY: `info` is provided by `libpmix`, and is valid for this function.
    let info = unsafe { slice_from_raw_parts(info, ninfo) };
    let ninfo_reqd = info
        .iter()
        .filter(|i| {
            (i.flags & sys::PMIX_INFO_REQD != 0) && (i.flags & sys::PMIX_INFO_REQD_PROCESSED == 0)
        })
        .count();
    println!(
        "fence_nb called: nprocs={} ninfo={} ({}) ndata={} cb={:?}",
        nprocs, ninfo, ninfo_reqd, ndata, cbfunc
    );
    if ninfo_reqd > 0 {
        return sys::PMIX_ERR_NOT_SUPPORTED;
    };
    #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
    let guard = PMIX_STATE.read().unwrap();

    if let Some(State::Server(ref s)) = *guard {
        // SAFETY: At least one proc must be participating in the fence, so procs must be valid
        let procs = unsafe { slice::from_raw_parts(procs, nprocs) }.into();
        let cb = (cbfunc, cbdata);
        let data = (data, ndata);
        // mpsc::UnboundedSender::send() only fails if the receiver is dropped,
        // which only happens in Server::drop, which clears PMIX_STATE and calls
        // PMIx_server_finalize (deactivating this callback).
        #[allow(clippy::unwrap_used, reason = "Unreachable if receiver is dropped")]
        s.send(Event::Fence { procs, data, cb }).unwrap();
        sys::PMIX_SUCCESS as sys::pmix_status_t
    } else {
        sys::PMIX_ERR_INIT as sys::pmix_status_t
    }
}

unsafe extern "C" fn direct_modex(
    proc: *const sys::pmix_proc_t,
    info: *const sys::pmix_info_t,
    ninfo: usize,
    cbfunc: sys::pmix_modex_cbfunc_t,
    cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    // SAFETY: `info` is provided by `libpmix`, and is valid for this function.
    let info = unsafe { slice_from_raw_parts(info, ninfo) };
    let ninfo_reqd = info
        .iter()
        .filter(|i| {
            (i.flags & sys::PMIX_INFO_REQD != 0) && (i.flags & sys::PMIX_INFO_REQD_PROCESSED == 0)
        })
        .count();
    println!("direct_modex called: ninfo={} ({})", info.len(), ninfo_reqd);
    if ninfo_reqd > 0 {
        return sys::PMIX_ERR_NOT_SUPPORTED;
    };
    #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
    let guard = PMIX_STATE.read().unwrap();

    if let Some(State::Server(ref s)) = *guard {
        // SAFETY: `proc` is passed to us by libpmix, assume it is valid.
        let proc = unsafe { *proc };
        let cb = (cbfunc, cbdata);
        // mpsc::UnboundedSender::send() only fails if the receiver is dropped,
        // which only happens in Server::drop, which clears PMIX_STATE and calls
        // PMIx_server_finalize (deactivating this callback).
        #[allow(clippy::unwrap_used, reason = "Unreachable if receiver is dropped")]
        s.send(Event::DirectModex { proc, cb }).unwrap();
        sys::PMIX_SUCCESS as sys::pmix_status_t
    } else {
        sys::PMIX_ERR_INIT as sys::pmix_status_t
    }
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
