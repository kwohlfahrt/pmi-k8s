use std::ffi::{self, CStr, c_void};
use std::fmt::Display;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use thiserror::Error;

use super::sys;

pub struct PmixStatus(pub sys::pmix_status_t);

const PMIX_SUCCESS: sys::pmix_status_t = sys::PMIX_SUCCESS as sys::pmix_status_t;

impl PmixStatus {
    pub fn check(&self) -> Result<(), PmixError> {
        match self.0 {
            PMIX_SUCCESS => Ok(()),
            sys::PMIX_OPERATION_SUCCEEDED => Ok(()),
            e => Err(PmixError(e)),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub struct PmixError(pub sys::pmix_status_t);

impl Display for PmixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PmixError({})", self.0))
    }
}

impl Drop for sys::pmix_value_t {
    fn drop(&mut self) {
        // SAFETY: This is the destructor for this type. It frees any nested
        // values, but does not release the top-level value.
        unsafe { sys::PMIx_Value_destruct(self) };
    }
}

impl Drop for sys::pmix_info_t {
    fn drop(&mut self) {
        // SAFETY: This is the destructor for this type. It frees any nested
        // values, but does not release the top-level value.
        unsafe { sys::PMIx_Info_destruct(self) };
    }
}

/// Connects a Rust type to its PMIx data-type tag.
///
/// # Safety
/// `TAG` must denote the `pmix_data_type_t` that corresponds to the Rust type,
/// and `load` and `store` must access the corresponding union member.
pub unsafe trait Tagged {
    const TAG: sys::pmix_data_type_t;

    fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> PmixStatus;
    /// # Safety: caller must ensure `src.type_ == Self::TAG`.
    unsafe fn load(src: &sys::pmix_value_t) -> &Self;
}

#[repr(transparent)]
pub struct Value<T>(sys::pmix_value_t, PhantomData<T>);

impl<T: Tagged> Value<T> {
    pub fn get(&self) -> &T {
        // SAFETY: tag is enforced during construction of Value<T>
        unsafe { T::load(&self.0) }
    }
}

impl<T: Tagged> From<&T> for Value<T> {
    fn from(value: &T) -> Self {
        let mut v = MaybeUninit::<sys::pmix_value_t>::uninit();
        let r = value.store(&mut v);
        assert!(r.check().is_ok());
        // SAFETY: initialized with `value.store`, and return code checked
        let v = unsafe { v.assume_init() };
        Self(v, PhantomData)
    }
}

#[derive(Debug, Error)]
#[error("expected type tag {expected} got: {actual}")]
pub struct TagMismatch {
    expected: sys::pmix_data_type_t,
    actual: sys::pmix_data_type_t,
}

impl<T: Tagged> TryFrom<sys::pmix_value_t> for Value<T> {
    type Error = TagMismatch;

    fn try_from(value: sys::pmix_value_t) -> Result<Self, Self::Error> {
        if value.type_ == T::TAG {
            Ok(Self(value, PhantomData))
        } else {
            Err(TagMismatch {
                expected: T::TAG,
                actual: value.type_,
            })
        }
    }
}

macro_rules! pmix_tagged_from {
    ($T:ty, $variant:ident, $tag:ident) => {
        // SAFETY: Macro invoked with the correct type/variant/tag
        unsafe impl Tagged for $T {
            const TAG: sys::pmix_data_type_t = sys::$tag as sys::pmix_data_type_t;

            fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> PmixStatus {
                // SAFETY: `data` is the correct type for tag, and is copied
                PmixStatus(unsafe {
                    sys::PMIx_Value_load(
                        dst.as_mut_ptr(),
                        self as *const $T as *const c_void,
                        Self::TAG,
                    )
                })
            }

            unsafe fn load(src: &sys::pmix_value_t) -> &Self {
                // SAFETY: Type invariant is that we have the correct tag
                unsafe { &src.data.$variant }
            }
        }
    };
}

pmix_tagged_from!(bool, flag, PMIX_BOOL);
pmix_tagged_from!(u32, uint32, PMIX_UINT32);

// SAFETY: Tag is correct for C-strings, and we access data.string
unsafe impl Tagged for ffi::CStr {
    const TAG: sys::pmix_data_type_t = sys::PMIX_STRING as sys::pmix_data_type_t;

    fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> PmixStatus {
        // SAFETY: `data` is the correct type for tag, and is copied
        PmixStatus(unsafe {
            sys::PMIx_Value_load(
                dst.as_mut_ptr(),
                self as *const CStr as *const c_void,
                Self::TAG,
            )
        })
    }

    unsafe fn load(src: &sys::pmix_value_t) -> &Self {
        // SAFETY: Type invariant is that we have the correct tag
        let string = unsafe { src.data.string };
        if string.is_null() {
            c""
        } else {
            // SAFETY: We've checked NULL, and PMIx stores a C string here.
            unsafe { CStr::from_ptr(string) }
        }
    }
}

/// # SAFETY
/// `KEY` must be the correct PMIx info key for the `Value` type.
pub unsafe trait Key {
    const KEY: &CStr;

    type Value: Tagged + ?Sized;

    fn store(value: &Self::Value, dst: &mut MaybeUninit<sys::pmix_info_t>) -> PmixStatus {
        // SAFETY: `data` is the correct type for `KEY`. The `tag` match is
        // enforced by the `Tagged` impl on `value.`
        PmixStatus(unsafe {
            sys::PMIx_Info_load(
                dst.as_mut_ptr(),
                Self::KEY.as_ptr(),
                value as *const Self::Value as *const c_void,
                Self::Value::TAG,
            )
        })
    }

    fn info(value: &Self::Value) -> sys::pmix_info_t {
        let mut v = MaybeUninit::<sys::pmix_info_t>::uninit();
        let r = Self::store(value, &mut v);
        assert!(r.check().is_ok());
        // SAFETY: initialized with `K::store`, and return code checked
        unsafe { v.assume_init() }
    }
}

macro_rules! pmix_info_key_from {
    ($S:ident, $T:ty, $tag:ident) => {
        pub struct $S();
        // SAFETY: Macro invoked with the correct type/tag
        unsafe impl Key for $S {
            const KEY: &CStr = sys::$tag;
            type Value = $T;
        }
    };
}

pmix_info_key_from!(JobSize, u32, PMIX_JOB_SIZE);
pmix_info_key_from!(UniverseSize, u32, PMIX_UNIV_SIZE);
pmix_info_key_from!(NodeMap, CStr, PMIX_NODE_MAP);
pmix_info_key_from!(ProcMap, CStr, PMIX_PROC_MAP);
pmix_info_key_from!(ServerTmpdir, CStr, PMIX_SERVER_TMPDIR);
pmix_info_key_from!(SystemTmpdir, CStr, PMIX_SYSTEM_TMPDIR);
pmix_info_key_from!(ServerSystemSupport, bool, PMIX_SERVER_SYSTEM_SUPPORT);

impl From<(&CStr, &CStr)> for sys::pmix_info_t {
    fn from((key, src): (&CStr, &CStr)) -> Self {
        let tag = sys::PMIX_STRING as u16;
        let key = key.as_ptr();
        let mut v = MaybeUninit::<Self>::uninit();
        // SAFETY: `data` is the correct type for tag, and is copied.
        // FIXME: `key` must match the value type, downstream assumes they are
        // as per the standard. We should enforce this at the type level.
        let status = unsafe {
            sys::PMIx_Info_load(
                v.as_mut_ptr(),
                key,
                src as *const CStr as *const c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        // SAFETY: v was initialized by PMIx_Info_load
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
        // SAFETY: `data` is the correct type for tag, and is copied
        let status = unsafe {
            sys::PMIx_Value_load(
                v.as_mut_ptr(),
                &array as *const sys::pmix_data_array_t as *const c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        // SAFETY: v was initialized by PMIx_Value_load
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
        // SAFETY: `data` is the correct type for tag, and is copied.
        // FIXME: `key` must match the value type, downstream assumes they are
        // as per the standard. We should enforce this at the type level.
        let status = unsafe {
            sys::PMIx_Info_load(
                v.as_mut_ptr(),
                key,
                &array as *const sys::pmix_data_array_t as *const c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        // SAFETY: v was initialized by PMIx_Info_load
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
                // SAFETY: `data` is the correct type for tag, and is copied
                let status = unsafe {
                    sys::PMIx_Value_load(v.as_mut_ptr(), src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                // SAFETY: v was initialized by PMIx_Value_load
                unsafe { v.assume_init() }
            }
        }

        impl From<(&CStr, $t)> for sys::pmix_info_t {
            fn from((key, src): (&CStr, $t)) -> Self {
                let tag = sys::$tag as u16;
                let src = &src;
                let key = key.as_ptr();
                let mut v = MaybeUninit::<Self>::uninit();
                // SAFETY: `data` is the correct type for tag, and is copied.
                // FIXME: `key` must match the value type, downstream assumes they are
                // as per the standard. We should enforce this at the type level.
                let status = unsafe {
                    sys::PMIx_Info_load(v.as_mut_ptr(), key, src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                // SAFETY: v was initialized by PMIx_Info_load
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
                // SAFETY: `data` is the correct type for tag, and is copied
                let status = unsafe {
                    sys::PMIx_Value_load(v.as_mut_ptr(), src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                // SAFETY: v was initialized by PMIx_Value_load
                unsafe { v.assume_init() }
            }
        }

        impl From<(&CStr, $newtype)> for sys::pmix_info_t {
            fn from((key, src): (&CStr, $newtype)) -> Self {
                let tag = sys::$tag as u16;
                let src = &src.0;
                let key = key.as_ptr();
                let mut v = MaybeUninit::<Self>::uninit();
                // SAFETY: `data` is the correct type for tag, and is copied.
                // FIXME: `key` must match the value type, downstream assumes they are
                // as per the standard. We should enforce this at the type level.
                let status = unsafe {
                    sys::PMIx_Info_load(v.as_mut_ptr(), key, src as *const $t as *const c_void, tag)
                };
                assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
                // SAFETY: v was initialized by PMIx_Info_load
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
