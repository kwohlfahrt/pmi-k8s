use std::{ffi::CStr, ptr};

pub mod sys;
mod value;

pub fn get_version_str() -> &'static CStr {
    unsafe { CStr::from_ptr(sys::PMIx_Get_version()) }
}

pub fn server_init(infos: &[sys::pmix_info_t]) {
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
}

pub fn register_namespace(nspace: &CStr) {
    let mut infos: [sys::pmix_info_t; _] = [
        (sys::PMIX_UNIV_SIZE, &1).into(),
        (sys::PMIX_JOB_SIZE, &1).into(),
        (sys::PMIX_LOCAL_SIZE, &1).into(),
    ];
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
}

pub fn register_client(namespace: &CStr, rank: u32) {
    let namespace = namespace.to_bytes_with_nul();

    let uid = nix::unistd::geteuid();
    let gid = nix::unistd::getegid();
    let mut nspace = [0; _];
    nspace[..namespace.len()].copy_from_slice(namespace);

    let proc = sys::pmix_proc_t { nspace, rank };

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
}

pub fn client_init(infos: &[sys::pmix_info_t]) {
    let status = unsafe {
        sys::PMIx_Init(
            std::ptr::null_mut(),
            infos.as_ptr() as *mut sys::pmix_info_t,
            infos.len(),
        )
    };
    assert_eq!(status, sys::PMIX_SUCCESS as i32);
}

pub fn is_initialized() -> bool {
    let status = unsafe { sys::PMIx_Initialized() };
    status != 0
}

#[cfg(test)]
mod test {
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial(server)]
    fn test_server_init() {
        assert!(!is_initialized());
        server_init(&mut []);
        assert!(is_initialized());
    }
}
