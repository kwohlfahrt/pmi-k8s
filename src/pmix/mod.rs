use std::ffi::CStr;

mod sys;

pub fn get_version_str() -> &'static CStr {
    unsafe { CStr::from_ptr(sys::PMIx_Get_version()) }
}
