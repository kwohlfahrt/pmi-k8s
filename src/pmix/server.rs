use std::ffi::{c_char, c_int, c_void, CString};
use std::ptr;
use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use super::bindings::*;
use crate::coordinator::FenceRequest;
use crate::kv_store::KvStore;

/// Events that the PMIx server sends to the main coordinator
#[derive(Debug)]
pub enum PmixEvent {
    ClientConnected {
        nspace: String,
        rank: u32,
    },
    ClientFinalized {
        nspace: String,
        rank: u32,
    },
    FenceRequest {
        request: FenceRequest,
        data: Vec<u8>,
        callback: FenceCallback,
    },
    DirectModexRequest {
        nspace: String,
        rank: u32,
        callback: ModexCallback,
    },
    Abort {
        nspace: String,
        rank: u32,
        status: i32,
        message: String,
    },
}

/// Callback wrapper for fence completion
pub struct FenceCallback {
    cbfunc: pmix_modex_cbfunc_t,
    cbdata: *mut c_void,
}

impl std::fmt::Debug for FenceCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FenceCallback")
            .field("has_callback", &self.cbfunc.is_some())
            .finish()
    }
}

unsafe impl Send for FenceCallback {}
unsafe impl Sync for FenceCallback {}

impl FenceCallback {
    pub fn complete(&self, status: pmix_status_t, data: &[u8]) {
        if let Some(func) = self.cbfunc {
            unsafe {
                func(
                    status,
                    data.as_ptr() as *const c_char,
                    data.len(),
                    self.cbdata,
                    None,
                    ptr::null_mut(),
                );
            }
        }
    }
}

/// Callback wrapper for direct modex
pub struct ModexCallback {
    cbfunc: pmix_modex_cbfunc_t,
    cbdata: *mut c_void,
}

impl std::fmt::Debug for ModexCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModexCallback")
            .field("has_callback", &self.cbfunc.is_some())
            .finish()
    }
}

unsafe impl Send for ModexCallback {}
unsafe impl Sync for ModexCallback {}

impl ModexCallback {
    pub fn complete(&self, status: pmix_status_t, data: &[u8]) {
        if let Some(func) = self.cbfunc {
            unsafe {
                func(
                    status,
                    data.as_ptr() as *const c_char,
                    data.len(),
                    self.cbdata,
                    None,
                    ptr::null_mut(),
                );
            }
        }
    }
}

/// Thread-safe state for the PMIx server callbacks
struct ServerState {
    event_tx: mpsc::UnboundedSender<PmixEvent>,
    kv_store: Arc<KvStore>,
}

// Global state - PMIx callbacks are C functions that need global access
// Using OnceLock for safe initialization (Rust 2024 compatible)
static SERVER_STATE: OnceLock<ServerState> = OnceLock::new();

fn get_state() -> Option<&'static ServerState> {
    SERVER_STATE.get()
}

/// PMIx Server wrapper
pub struct PmixServer {
    _module: Box<pmix_server_module_t>,
}

impl PmixServer {
    /// Initialize the PMIx server
    pub fn new(
        event_tx: mpsc::UnboundedSender<PmixEvent>,
        kv_store: Arc<KvStore>,
        tmpdir: &str,
    ) -> Result<Self, PmixError> {
        // Store global state for callbacks
        let state = ServerState { event_tx, kv_store };
        SERVER_STATE
            .set(state)
            .map_err(|_| PmixError::AlreadyInitialized)?;

        // Create the server module with our callbacks
        let mut module = Box::new(pmix_server_module_t {
            client_connected: Some(client_connected_cb),
            client_finalized: Some(client_finalized_cb),
            abort: Some(abort_cb),
            fence_nb: Some(fence_nb_cb),
            direct_modex: Some(direct_modex_cb),
            publish: None,    // Not supported
            lookup: None,     // Not supported
            unpublish: None,  // Not supported
            spawn: None,      // Not supported
            connect: None,    // Not supported
            disconnect: None, // Not supported
            register_events: None,
            deregister_events: None,
            listener: None,
            notify_event: None,
            query: None,
            tool_connected: None,
            log: None,
            allocate: None,
            job_control: None,
            monitor: None,
            get_credential: None,
            validate_credential: None,
            iof_pull: None,
            push_stdin: None,
            group: None,
            fabric: None,
            client_connected2: None,
            session_control: None,
        });

        // Prepare initialization info
        let tmpdir_cstr = CString::new(tmpdir).map_err(|_| PmixError::InvalidString)?;

        // Create info array for server tmpdir
        let mut info = vec![pmix_info_t {
            key: str_to_key("pmix.srvr.tmpdir"),
            flags: 0,
            value: pmix_value_t {
                type_: PMIX_STRING as u16,
                data: pmix_value__bindgen_ty_1 {
                    string: tmpdir_cstr.into_raw(),
                },
            },
        }];

        let status = unsafe {
            PMIx_server_init(module.as_mut() as *mut _, info.as_mut_ptr(), info.len())
        };

        if !is_success(status) {
            error!("PMIx_server_init failed: {}", status_string(status));
            return Err(PmixError::InitFailed(status));
        }

        info!("PMIx server initialized successfully");
        Ok(PmixServer { _module: module })
    }

    /// Register a namespace with the PMIx server
    pub fn register_nspace(
        &self,
        nspace: &str,
        num_procs: u32,
        job_info: &[(String, String)],
    ) -> Result<(), PmixError> {
        let nspace_arr = str_to_nspace(nspace);

        // Build info array from job_info
        let mut infos: Vec<pmix_info_t> = job_info
            .iter()
            .map(|(key, value)| {
                let value_cstr = CString::new(value.as_str()).unwrap();
                pmix_info_t {
                    key: str_to_key(key),
                    flags: 0,
                    value: pmix_value_t {
                        type_: PMIX_STRING as u16,
                        data: pmix_value__bindgen_ty_1 {
                            string: value_cstr.into_raw(),
                        },
                    },
                }
            })
            .collect();

        let status = unsafe {
            PMIx_server_register_nspace(
                nspace_arr.as_ptr(),
                num_procs as i32,
                infos.as_mut_ptr(),
                infos.len(),
                None,
                ptr::null_mut(),
            )
        };

        if !is_success(status) {
            error!(
                "PMIx_server_register_nspace failed: {}",
                status_string(status)
            );
            return Err(PmixError::RegisterFailed(status));
        }

        info!(nspace, num_procs, "Registered namespace");
        Ok(())
    }

    /// Register a client with the PMIx server
    pub fn register_client(&self, nspace: &str, rank: u32) -> Result<(), PmixError> {
        let proc = make_proc(nspace, rank);

        let status = unsafe {
            PMIx_server_register_client(&proc as *const _, 0, 0, ptr::null_mut(), None, ptr::null_mut())
        };

        if !is_success(status) {
            error!(
                "PMIx_server_register_client failed: {}",
                status_string(status)
            );
            return Err(PmixError::RegisterFailed(status));
        }

        debug!(nspace, rank, "Registered client");
        Ok(())
    }

    /// Setup environment for a forked process
    pub fn setup_fork(&self, nspace: &str, rank: u32) -> Result<Vec<(String, String)>, PmixError> {
        let proc = make_proc(nspace, rank);
        let mut env_ptr: *mut *mut c_char = ptr::null_mut();

        let status = unsafe { PMIx_server_setup_fork(&proc as *const _, &mut env_ptr) };

        if !is_success(status) {
            error!("PMIx_server_setup_fork failed: {}", status_string(status));
            return Err(PmixError::SetupFailed(status));
        }

        // Convert the C string array to Rust
        let mut env_vars = Vec::new();
        if !env_ptr.is_null() {
            unsafe {
                let mut i = 0;
                while !(*env_ptr.add(i)).is_null() {
                    let cstr = std::ffi::CStr::from_ptr(*env_ptr.add(i));
                    let s = cstr.to_string_lossy();
                    if let Some((key, value)) = s.split_once('=') {
                        env_vars.push((key.to_string(), value.to_string()));
                    }
                    i += 1;
                }
            }
        }

        Ok(env_vars)
    }
}

impl Drop for PmixServer {
    fn drop(&mut self) {
        info!("Finalizing PMIx server");
        unsafe {
            PMIx_server_finalize();
            // Note: OnceLock cannot be cleared, but since we're dropping the server,
            // the program is likely shutting down anyway
        }
    }
}

// === PMIx Server Callbacks ===

extern "C" fn client_connected_cb(
    proc: *const pmix_proc_t,
    _server_object: *mut c_void,
    cbfunc: pmix_op_cbfunc_t,
    cbdata: *mut c_void,
) -> pmix_status_t {
    let Some(state) = get_state() else {
        return PMIX_ERR_INIT as i32;
    };

    let (nspace, rank) = unsafe {
        let proc = &*proc;
        (nspace_to_string(&proc.nspace), proc.rank)
    };

    info!(nspace, rank, "Client connected");

    let _ = state.event_tx.send(PmixEvent::ClientConnected { nspace, rank });

    // Call the callback to unblock the client
    if let Some(func) = cbfunc {
        unsafe {
            func(PMIX_SUCCESS as i32, cbdata);
        }
    }

    PMIX_SUCCESS as i32
}

extern "C" fn client_finalized_cb(
    proc: *const pmix_proc_t,
    _server_object: *mut c_void,
    cbfunc: pmix_op_cbfunc_t,
    cbdata: *mut c_void,
) -> pmix_status_t {
    let Some(state) = get_state() else {
        return PMIX_ERR_INIT as i32;
    };

    let (nspace, rank) = unsafe {
        let proc = &*proc;
        (nspace_to_string(&proc.nspace), proc.rank)
    };

    info!(nspace, rank, "Client finalized");

    let _ = state.event_tx.send(PmixEvent::ClientFinalized { nspace, rank });

    // Call the callback to unblock the client
    if let Some(func) = cbfunc {
        unsafe {
            func(PMIX_SUCCESS as i32, cbdata);
        }
    }

    PMIX_SUCCESS as i32
}

extern "C" fn abort_cb(
    proc: *const pmix_proc_t,
    _server_object: *mut c_void,
    status: c_int,
    msg: *const c_char,
    _procs: *mut pmix_proc_t,
    _nprocs: usize,
    cbfunc: pmix_op_cbfunc_t,
    cbdata: *mut c_void,
) -> pmix_status_t {
    let Some(state) = get_state() else {
        return PMIX_ERR_INIT as i32;
    };

    let (nspace, rank) = unsafe {
        let proc = &*proc;
        (nspace_to_string(&proc.nspace), proc.rank)
    };

    let message = if msg.is_null() {
        String::new()
    } else {
        unsafe { std::ffi::CStr::from_ptr(msg).to_string_lossy().into_owned() }
    };

    error!(nspace, rank, status, message, "Client aborted");

    let _ = state.event_tx.send(PmixEvent::Abort {
        nspace,
        rank,
        status,
        message,
    });

    // Call the callback
    if let Some(func) = cbfunc {
        unsafe {
            func(PMIX_SUCCESS as i32, cbdata);
        }
    }

    PMIX_SUCCESS as i32
}

extern "C" fn fence_nb_cb(
    procs: *const pmix_proc_t,
    nprocs: usize,
    _info: *const pmix_info_t,
    _ninfo: usize,
    data: *mut c_char,
    ndata: usize,
    cbfunc: pmix_modex_cbfunc_t,
    cbdata: *mut c_void,
) -> pmix_status_t {
    let Some(state) = get_state() else {
        return PMIX_ERR_INIT as i32;
    };

    // Extract participating procs
    let mut participants = Vec::with_capacity(nprocs);
    unsafe {
        for i in 0..nprocs {
            let proc = &*procs.add(i);
            participants.push((nspace_to_string(&proc.nspace), proc.rank));
        }
    }

    // Copy the fence data
    let fence_data = if data.is_null() || ndata == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data as *const u8, ndata).to_vec() }
    };

    debug!(
        participants = ?participants,
        data_len = fence_data.len(),
        "Fence request received"
    );

    // Create fence request
    let request = FenceRequest { participants };
    let callback = FenceCallback { cbfunc, cbdata };

    let _ = state.event_tx.send(PmixEvent::FenceRequest {
        request,
        data: fence_data,
        callback,
    });

    PMIX_SUCCESS as i32
}

extern "C" fn direct_modex_cb(
    proc: *const pmix_proc_t,
    _info: *const pmix_info_t,
    _ninfo: usize,
    cbfunc: pmix_modex_cbfunc_t,
    cbdata: *mut c_void,
) -> pmix_status_t {
    let Some(state) = get_state() else {
        return PMIX_ERR_INIT as i32;
    };

    let (nspace, rank) = unsafe {
        let proc = &*proc;
        (nspace_to_string(&proc.nspace), proc.rank)
    };

    debug!(nspace, rank, "Direct modex request");

    // First try to get from local store
    if let Some(data) = state.kv_store.get_modex_data(&nspace, rank) {
        let callback = ModexCallback { cbfunc, cbdata };
        callback.complete(PMIX_SUCCESS as i32, &data);
        return PMIX_SUCCESS as i32;
    }

    // Otherwise, send request to coordinator for remote fetch
    let callback = ModexCallback { cbfunc, cbdata };
    let _ = state.event_tx.send(PmixEvent::DirectModexRequest {
        nspace,
        rank,
        callback,
    });

    PMIX_SUCCESS as i32
}

/// PMIx error type
#[derive(Debug, thiserror::Error)]
pub enum PmixError {
    #[error("PMIx initialization failed: {}", status_string(*.0))]
    InitFailed(pmix_status_t),
    #[error("PMIx registration failed: {}", status_string(*.0))]
    RegisterFailed(pmix_status_t),
    #[error("PMIx setup failed: {}", status_string(*.0))]
    SetupFailed(pmix_status_t),
    #[error("Invalid string")]
    InvalidString,
    #[error("PMIx server already initialized")]
    AlreadyInitialized,
}
