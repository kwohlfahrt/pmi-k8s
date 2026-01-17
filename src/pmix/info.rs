use std::ffi::{CStr, c_void};

use super::sys;

impl Drop for sys::pmix_info_t {
    fn drop(&mut self) {
        unsafe {sys::PMIx_Info_destruct(self)};
    }
}
