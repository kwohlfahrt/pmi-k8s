use std::ffi::CStr;

pub mod sys;
mod value;

use value::Info;

pub fn get_version_str() -> &'static CStr {
    unsafe { CStr::from_ptr(sys::PMIx_Get_version()) }
}

pub fn server_init(infos: &mut [Info]) {
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
            infos.as_mut_ptr() as *mut sys::pmix_info_t,
            infos.len(),
        )
    };
    assert_eq!(status, sys::PMIX_SUCCESS as i32);
}
