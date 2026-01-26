use std::ffi::{CStr, c_void};
use std::mem::MaybeUninit;

use super::sys;

impl Drop for sys::pmix_value_t {
    fn drop(&mut self) {
        unsafe { sys::PMIx_Value_destruct(self) };
    }
}

impl Drop for sys::pmix_info_t {
    fn drop(&mut self) {
        unsafe { sys::PMIx_Info_destruct(self) };
    }
}

impl From<&CStr> for sys::pmix_value_t {
    fn from(src: &CStr) -> Self {
        let tag = sys::PMIX_STRING as u16;
        let mut v = MaybeUninit::<Self>::uninit();
        // PMIx_Value_load copies data out of src
        let status = unsafe {
            sys::PMIx_Value_load(v.as_mut_ptr(), src as *const CStr as *const c_void, tag)
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        unsafe { v.assume_init() }
    }
}

impl From<(&CStr, &CStr)> for sys::pmix_info_t {
    fn from((key, src): (&CStr, &CStr)) -> Self {
        let tag = sys::PMIX_STRING as u16;
        let key = key.as_ptr();
        let mut v = MaybeUninit::<Self>::uninit();
        // PMIx_Info_load copies data out of src
        let status = unsafe {
            sys::PMIx_Info_load(
                v.as_mut_ptr(),
                key,
                src as *const CStr as *const c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        unsafe { v.assume_init() }
    }
}

impl From<&[sys::pmix_value_t]> for sys::pmix_value_t {
    fn from(src: &[sys::pmix_value_t]) -> Self {
        let tag = sys::PMIX_DATA_ARRAY as u16;
        let array = sys::pmix_data_array_t {
            type_: sys::PMIX_VALUE as u16,
            size: src.len(),
            array: src.as_ptr() as *mut c_void,
        };

        let mut v = MaybeUninit::<Self>::uninit();
        // PMIx_Value_load copies data out of src
        let status = unsafe {
            sys::PMIx_Value_load(
                v.as_mut_ptr(),
                &array as *const sys::pmix_data_array_t as *const c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        unsafe { v.assume_init() }
    }
}

impl From<(&CStr, &[sys::pmix_info_t])> for sys::pmix_info_t {
    fn from((key, src): (&CStr, &[sys::pmix_info_t])) -> Self {
        let tag = sys::PMIX_DATA_ARRAY as u16;
        let key = key.as_ptr();
        let array = sys::pmix_data_array_t {
            type_: sys::PMIX_INFO as u16,
            size: src.len(),
            array: src.as_ptr() as *mut c_void,
        };

        let mut v = MaybeUninit::<Self>::uninit();
        // PMIx_Info_load copies data out of src
        let status = unsafe {
            sys::PMIx_Info_load(
                v.as_mut_ptr(),
                key,
                &array as *const sys::pmix_data_array_t as *const c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        unsafe { v.assume_init() }
    }
}

macro_rules! pmix_value_from {
    ($t:ty, $variant:ident, $tag:ident) => {
        impl From<$t> for sys::pmix_value_t {
            fn from(src: $t) -> Self {
                let src = &src;
                let tag = sys::$tag as u16;
                let mut v = MaybeUninit::<Self>::uninit();
                // PMIx_Value_load copies data out of src
                let status = unsafe {
                    sys::PMIx_Value_load(v.as_mut_ptr(), src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                unsafe { v.assume_init() }
            }
        }

        impl From<(&CStr, $t)> for sys::pmix_info_t {
            fn from((key, src): (&CStr, $t)) -> Self {
                let tag = sys::$tag as u16;
                let src = &src;
                let key = key.as_ptr();
                let mut v = MaybeUninit::<Self>::uninit();
                // PMIx_Info_load copies data out of src
                let status = unsafe {
                    sys::PMIx_Info_load(v.as_mut_ptr(), key, src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                unsafe { v.assume_init() }
            }
        }
    };
}

macro_rules! pmix_value_from_newtype {
    ($t:ty, $newtype:ident, $variant:ident, $tag:ident) => {
        pub struct $newtype(pub $t);

        impl From<$newtype> for sys::pmix_value_t {
            fn from(src: $newtype) -> Self {
                let src = &src.0;
                let tag = sys::$tag as u16;
                let mut v = MaybeUninit::<Self>::uninit();
                // PMIx_Value_load copies data out of src
                let status = unsafe {
                    sys::PMIx_Value_load(v.as_mut_ptr(), src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                unsafe { v.assume_init() }
            }
        }

        impl From<(&CStr, $newtype)> for sys::pmix_info_t {
            fn from((key, src): (&CStr, $newtype)) -> Self {
                let tag = sys::$tag as u16;
                let src = &src.0;
                let key = key.as_ptr();
                let mut v = MaybeUninit::<Self>::uninit();
                // PMIx_Info_load copies data out of src
                let status = unsafe {
                    sys::PMIx_Info_load(v.as_mut_ptr(), key, src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                unsafe { v.assume_init() }
            }
        }
    };
}

pmix_value_from!(bool, flag, PMIX_BOOL);
pmix_value_from_newtype!(u8, Byte, byte, PMIX_BYTE);
pmix_value_from!(usize, size, PMIX_SIZE);
pmix_value_from_newtype!(libc::pid_t, Pid, pid, PMIX_PID);
pmix_value_from_newtype!(libc::c_int, Int, pid, PMIX_PID);
pmix_value_from!(i8, int8, PMIX_INT8);
pmix_value_from!(i16, int16, PMIX_INT16);
pmix_value_from!(i32, int32, PMIX_INT32);
pmix_value_from!(i64, int64, PMIX_INT64);
pmix_value_from_newtype!(libc::c_uint, UInt, uint, PMIX_UINT64);
pmix_value_from!(u8, uint8, PMIX_UINT8);
pmix_value_from!(u16, uint16, PMIX_UINT16);
pmix_value_from!(u32, uint32, PMIX_UINT32);
pmix_value_from!(u64, uint64, PMIX_UINT64);
pmix_value_from!(f32, fval, PMIX_FLOAT);
pmix_value_from!(f64, dval, PMIX_DOUBLE);
pmix_value_from!(sys::timeval, tv, PMIX_TIMEVAL);
pmix_value_from_newtype!(sys::time_t, Time, time, PMIX_TIME);
// pmix_value_from_newtype!(sys::pmix_status_t, Status, status, PMIX_STATUS);
pmix_value_from_newtype!(sys::pmix_rank_t, Rank, rank, PMIX_PROC_RANK);
// pmix_proc_t *proc; // version 2.025
pmix_value_from!(sys::pmix_byte_object_t, bo, PMIX_BYTE_OBJECT);
// pmix_value_from!(sys::pmix_persistence_t, persist, PMIX_PERSIST);
// pmix_value_from!(sys::pmix_scope_t, scope, PMIX_SCOPE);
// pmix_value_from!(sys::pmix_data_range_t, range, PMIX_DATA_RANGE);
// pmix_value_from!(sys::pmix_proc_state_t, state, PMIX_PROC_STATE);
// pmix_proc_info_t *pinfo; // version 2.031
// pmix_data_array_t *darray; // version 2.032
// void *ptr; // version 2.033
// pmix_value_from!(sys::pmix_alloc_directive_t, adir, PMIX_ALLOC_DIRECTIVE);
