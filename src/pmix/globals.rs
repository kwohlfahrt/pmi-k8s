#[cfg(test)]
use std::ptr;

use std::{ffi, ops::Deref, slice, sync::RwLock};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::pmix::{char_to_u8, u8_to_char};

use super::{slice_from_raw_parts, sys, value::PmixError};

pub struct ModexCallback(sys::pmix_modex_cbfunc_t, *mut ffi::c_void);

// SAFETY: A single-use callback + data.
unsafe impl Send for ModexCallback {}

impl ModexCallback {
    pub fn call(self, status: sys::pmix_status_t, data: Vec<u8>) {
        let Some(cbfunc) = self.0 else {
            return;
        };

        let data = Box::new(data);
        let char_data = u8_to_char(&data);

        // SAFETY: `char_data` lives as long as `data`, which is freed by libpmix using `release_vec_u8`.
        unsafe {
            cbfunc(
                status,
                char_data.as_ptr(),
                char_data.len(),
                self.1,
                Some(release_vec_u8),
                Box::into_raw(data) as *mut ffi::c_void,
            )
        }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        Self(None, ptr::null_mut())
    }

    #[cfg(test)]
    pub fn test_callback(cb: Box<TestCb>) -> Self {
        let cb = Box::new(cb);
        Self(Some(test_cbfunc), Box::into_raw(cb) as *mut ffi::c_void)
    }
}

#[cfg(test)]
type TestCb = dyn FnOnce(sys::pmix_status_t, &[u8]);

#[cfg(test)]
unsafe extern "C" fn test_cbfunc(
    status: sys::pmix_status_t,
    data: *const ffi::c_char,
    ndata: usize,
    cbdata: *mut ffi::c_void,
    release_fn: sys::pmix_release_cbfunc_t,
    release_cbdata: *mut ffi::c_void,
) {
    // SAFETY: Passed in from modex functions
    let data = unsafe { slice_from_raw_parts(data, ndata) };
    // SAFETY: Constructed in ModexCallback::test_callback
    let cb = unsafe { Box::from_raw(cbdata as *mut Box<TestCb>) };
    cb(status, char_to_u8(data));
    if let Some(release_fn) = release_fn {
        // SAFETY: Passed in from modex functions
        unsafe { release_fn(release_cbdata) }
    }
}

pub struct CData(*mut ffi::c_char, usize);

// SAFETY: Just a bunch of (read-only) bytes.
unsafe impl Send for CData {}

// SAFETY: Just a bunch of (read-only) bytes.
unsafe impl Sync for CData {}

impl CData {
    /// # Safety
    ///
    /// `ptr` must be allocated with `libc::malloc`, and point to `size` bytes.
    /// We take ownership of `ptr` and `libc::free` it on drop.
    pub unsafe fn from_raw_parts(ptr: *mut ffi::c_char, size: usize) -> Self {
        Self(ptr, size)
    }

    #[cfg(test)]
    #[allow(clippy::undocumented_unsafe_blocks)]
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        let len = bytes.len();
        let ptr = unsafe { libc::malloc(len) as *mut u8 };
        if ptr.is_null() {
            None
        } else {
            unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, len) };
            Some(Self(ptr as *mut ffi::c_char, len))
        }
    }
}

impl Deref for CData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: We own the data for our own lifetime. `slice_from_raw_parts`
        // handles the NULL check for us, and there are no alignment
        // requirements for `u8`.
        let data = unsafe { slice_from_raw_parts(self.0, self.1) };
        char_to_u8(data)
    }
}

impl Drop for CData {
    fn drop(&mut self) {
        // SAFETY: We are responsible for free'ing the data passed to us. I
        // assume this means libc::free.
        unsafe { libc::free(self.0 as *mut ffi::c_void) }
    }
}

pub struct FenceEvent {
    pub procs: Vec<sys::pmix_proc_t>,
    pub data: CData,
    pub cb: ModexCallback,
}

pub struct DirectModexEvent {
    pub proc: sys::pmix_proc_t,
    pub cb: ModexCallback,
}

pub enum State {
    Client,
    Server {
        fence_tx: mpsc::UnboundedSender<FenceEvent>,
        modex_tx: mpsc::UnboundedSender<DirectModexEvent>,
    },
}

pub static PMIX_STATE: RwLock<Option<State>> = RwLock::new(None);

#[derive(thiserror::Error, Debug)]
pub enum InitError {
    #[error("PMIx operation returned error code {}", 0.0)]
    PmixError(#[from] PmixError),
    #[error("PMIx global state was already initialized")]
    AlreadyInitialized,
}

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
    info!("client_connected2 called, ninfo: {}", ninfo);
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
    // SAFETY: According to the standard, we (the host) are responsible for
    // free'ing the data passed to `fence_nb`.
    let data = unsafe { CData::from_raw_parts(data, ndata) };
    let cb = ModexCallback(cbfunc, cbdata);

    // SAFETY: `info` is provided by `libpmix`, and is valid for this function.
    let info = unsafe { slice_from_raw_parts(info, ninfo) };
    let ninfo_reqd = info
        .iter()
        .filter(|i| {
            (i.flags & sys::PMIX_INFO_REQD != 0) && (i.flags & sys::PMIX_INFO_REQD_PROCESSED == 0)
        })
        .count();
    info!(
        "fence_nb called: nprocs={} ninfo={} ({}) ndata={} cb={:?}",
        nprocs, ninfo, ninfo_reqd, ndata, cbfunc
    );
    if ninfo_reqd > 0 {
        return sys::PMIX_ERR_NOT_SUPPORTED;
    };
    #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
    let guard = PMIX_STATE.read().unwrap();

    if let Some(State::Server { ref fence_tx, .. }) = *guard {
        if procs.is_null() {
            sys::PMIX_ERR_INVALID_ARG
        } else {
            // SAFETY: We have just checked that procs is valid
            let procs = unsafe { slice::from_raw_parts(procs, nprocs) }.into();
            match fence_tx.send(FenceEvent { procs, data, cb }) {
                Ok(()) => sys::PMIX_SUCCESS as sys::pmix_status_t,
                Err(err) => {
                    warn!(%err, "error queueing fence");
                    sys::PMIX_ERROR
                }
            }
        }
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
    info!("direct_modex called: ninfo={} ({})", info.len(), ninfo_reqd);
    if ninfo_reqd > 0 {
        return sys::PMIX_ERR_NOT_SUPPORTED;
    };
    #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
    let guard = PMIX_STATE.read().unwrap();

    if let Some(State::Server { ref modex_tx, .. }) = *guard {
        // SAFETY: `proc` is passed to us by libpmix, assume it is valid.
        let proc = unsafe { *proc };
        if proc.rank > sys::PMIX_RANK_VALID {
            // TODO: Support job-level modex
            sys::PMIX_ERR_NOT_SUPPORTED
        } else {
            let cb = ModexCallback(cbfunc, cbdata);
            match modex_tx.send(DirectModexEvent { proc, cb }) {
                Ok(()) => sys::PMIX_SUCCESS as sys::pmix_status_t,
                Err(err) => {
                    warn!(%err, "error queueing modex");
                    sys::PMIX_ERROR
                }
            }
        }
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
    info!("publish called");
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
    info!("lookup called");
    sys::PMIX_ERR_NOT_SUPPORTED as sys::pmix_status_t
}

unsafe extern "C" fn query(
    _proct: *mut sys::pmix_proc_t,
    _queries: *mut sys::pmix_query_t,
    _nqueries: usize,
    _cbfunc: sys::pmix_info_cbfunc_t,
    _cbdata: *mut std::ffi::c_void,
) -> sys::pmix_status_t {
    info!("query called");
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
