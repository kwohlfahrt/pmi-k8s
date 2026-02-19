use std::{ffi, slice};

#[cfg(feature = "test-bins")]
pub mod client;
pub mod env;
pub mod globals;
pub mod server;
pub mod sys;
mod value;

pub fn get_version_str() -> &'static ffi::CStr {
    // SAFETY: PMIx_Get_version returns a statically allocated C string.
    unsafe { ffi::CStr::from_ptr(sys::PMIx_Get_version()) }
}

pub fn is_initialized() -> bool {
    // SAFETY: No concerns for PMIx_Initialized.
    let status = unsafe { sys::PMIx_Initialized() };
    status != 0
}

pub fn char_to_u8(chars: &[ffi::c_char]) -> &[u8] {
    let ptr = chars.as_ptr();
    // SAFETY: This is the recommended way to transmute [ffi::c_char] to [u8]
    #[cfg_attr(not(target_arch = "x86_64"), expect(clippy::unnecessary_cast))]
    unsafe {
        slice::from_raw_parts(ptr as *const u8, chars.len())
    }
}

pub fn u8_to_char(bytes: &[u8]) -> &[ffi::c_char] {
    let ptr = bytes.as_ptr();
    // SAFETY: This is the recommended way to transmute [u8] to [ffi::c_char]
    unsafe { slice::from_raw_parts(ptr as *const ffi::c_char, bytes.len()) }
}

/// # SAFETY
///
/// Safety requirements are exactly as for `std::slice::from_raw_parts`, except
/// `data` may be `NULL`.
pub unsafe fn slice_from_raw_parts<'a, T>(data: *const T, ndata: usize) -> &'a [T] {
    if !data.is_null() {
        // SAFETY: satisfied by this function's safety requirements.
        unsafe { slice::from_raw_parts(data, ndata) }
    } else {
        &[]
    }
}
