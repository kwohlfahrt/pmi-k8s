use std::ffi;

pub mod client;
pub mod env;
pub mod globals;
pub mod server;
pub mod sys;
mod value;

pub fn get_version_str() -> &'static ffi::CStr {
    unsafe { ffi::CStr::from_ptr(sys::PMIx_Get_version()) }
}

pub fn is_initialized() -> bool {
    let status = unsafe { sys::PMIx_Initialized() };
    status != 0
}
