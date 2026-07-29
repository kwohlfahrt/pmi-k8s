use core::slice;
use std::ffi;
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
/// Types implementing `Tagged` must have the same representation as the data
/// array element representation.
pub unsafe trait Tagged {
    const TAG: sys::pmix_data_type_t;

    fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> Result<(), PmixError>;
    /// # Safety: caller must ensure `src.type_ == Self::TAG`.
    unsafe fn load(src: &sys::pmix_value_t) -> &Self;

    fn tag_matches(src: &sys::pmix_value_t) -> bool {
        src.type_ == Self::TAG
    }
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
        assert!(r.is_ok());
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
        if T::tag_matches(&value) {
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
    ($T:ty, $variant:ident, $tag:expr) => {
        // SAFETY: Macro invoked with the correct type/variant/tag
        unsafe impl Tagged for $T {
            const TAG: sys::pmix_data_type_t = $tag as _;

            fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> Result<(), PmixError> {
                // SAFETY: `data` is the correct type for tag, and is copied
                PmixStatus(unsafe {
                    sys::PMIx_Value_load(
                        dst.as_mut_ptr(),
                        self as *const $T as *const ffi::c_void,
                        Self::TAG,
                    )
                })
                .check()
            }

            unsafe fn load(src: &sys::pmix_value_t) -> &Self {
                // SAFETY: Type invariant is that we have the correct tag
                unsafe { &src.data.$variant }
            }
        }
    };
}

macro_rules! pmix_tagged_from_newtype {
    ($T:ty, $N:ident, $variant:ident, $tag:expr) => {
        #[repr(transparent)]
        pub struct $N(pub $T);

        // SAFETY: Macro invoked with the correct type/variant/tag
        unsafe impl Tagged for $N {
            const TAG: sys::pmix_data_type_t = $tag as _;

            fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> Result<(), PmixError> {
                // SAFETY: `data` is the correct type for tag, and is copied
                PmixStatus(unsafe {
                    sys::PMIx_Value_load(
                        dst.as_mut_ptr(),
                        self as *const $N as *const ffi::c_void,
                        Self::TAG,
                    )
                })
                .check()
            }

            unsafe fn load(src: &sys::pmix_value_t) -> &Self {
                // SAFETY: Type invariant is that we have the correct tag
                let data = std::ptr::from_ref(&unsafe { src.data.$variant }).cast::<$N>();

                // SAFETY: $N is #[repr(transparent)] over $T
                unsafe { &*data }
            }
        }
    };
}

pmix_tagged_from!(bool, flag, sys::PMIX_BOOL);
pmix_tagged_from!(u16, uint16, sys::PMIX_UINT16);
pmix_tagged_from!(u32, uint32, sys::PMIX_UINT32);
pmix_tagged_from_newtype!(sys::pmix_rank_t, Rank, rank, sys::PMIX_PROC_RANK);

#[repr(transparent)]
pub struct PmixStr(*const ffi::c_char);

impl From<&PmixStr> for &ffi::CStr {
    fn from(value: &PmixStr) -> Self {
        if value.0.is_null() {
            c""
        } else {
            unsafe { ffi::CStr::from_ptr(value.0) }
        }
    }
}

impl From<&ffi::CStr> for &PmixStr {
    fn from(value: &ffi::CStr) -> Self {
        let p = value.as_ptr().cast::<PmixStr>();
        unsafe { &*p }
    }
}

// SAFETY: Tag is correct for C-strings, and we access data.string
unsafe impl Tagged for PmixStr {
    const TAG: sys::pmix_data_type_t = sys::PMIX_STRING as _;

    fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> Result<(), PmixError> {
        // SAFETY: `data` is the correct type for tag, and is copied
        PmixStatus(unsafe {
            sys::PMIx_Value_load(dst.as_mut_ptr(), self.0 as *const ffi::c_void, Self::TAG)
        })
        .check()
    }

    unsafe fn load(src: &sys::pmix_value_t) -> &Self {
        // SAFETY: Type invariant is that we have the correct tag
        let string = unsafe { src.data.string }.cast::<PmixStr>();
        // SAFETY: PmixStr is #[repr(transparent)]
        unsafe { &*string }
    }
}

// SAFETY: Tag is correct for the top-level array, and recursively for inner data
unsafe impl<T: Tagged> Tagged for [T] {
    const TAG: sys::pmix_data_type_t = sys::PMIX_DATA_ARRAY as _;

    fn store(&self, dst: &mut MaybeUninit<sys::pmix_value_t>) -> Result<(), PmixError> {
        let array = sys::pmix_data_array_t {
            type_: T::TAG,
            size: self.len(),
            array: self.as_ptr() as *mut ffi::c_void,
        };

        let dst_p = dst.as_mut_ptr();

        // SAFETY: PMIx_Value_load dispatches based on the array type_ tag.
        PmixStatus(unsafe {
            sys::PMIx_Value_load(
                dst_p,
                &array as *const sys::pmix_data_array_t as *const ffi::c_void,
                Self::TAG,
            )
        })
        .check()?;

        // SAFETY: We've just constructed this variant
        let array = unsafe { dst_p.as_ref_unchecked().data.darray };
        if !self.is_empty() && array.is_null() {
            Err(PmixError(sys::PMIX_ERROR))
        } else {
            Ok(())
        }
    }

    unsafe fn load(src: &sys::pmix_value_t) -> &Self {
        // SAFETY: Type invariant is that we have the correct tag
        let array = unsafe { src.data.darray.as_ref() };

        if let Some(array) = array
            && !array.array.is_null()
            && array.size > 0
        {
            // SAFETY: Type invariant is that the tag is (recursively) correct
            unsafe { slice::from_raw_parts(array.array as *mut T, array.size) }
        } else {
            &[]
        }
    }

    fn tag_matches(src: &sys::pmix_value_t) -> bool {
        if src.type_ == Self::TAG {
            // SAFETY: Type invariant is that we have the correct tag
            let array = unsafe { src.data.darray.as_ref() };
            array.is_none_or(|a| a.type_ == T::TAG)
        } else {
            false
        }
    }
}

impl From<(&ffi::CStr, &[sys::pmix_info_t])> for sys::pmix_info_t {
    fn from((key, src): (&ffi::CStr, &[sys::pmix_info_t])) -> Self {
        let tag = sys::PMIX_DATA_ARRAY as u16;
        let key = key.as_ptr();
        let array = sys::pmix_data_array_t {
            type_: sys::PMIX_INFO as u16,
            size: src.len(),
            array: src.as_ptr() as *mut ffi::c_void,
        };

        let mut v = MaybeUninit::<Self>::uninit();
        // SAFETY: `data` is the correct type for tag, and is copied.
        // FIXME: `key` must match the value type, downstream assumes they are
        // as per the standard. We should enforce this at the type level.
        let status = unsafe {
            sys::PMIx_Info_load(
                v.as_mut_ptr(),
                key,
                &array as *const sys::pmix_data_array_t as *const ffi::c_void,
                tag,
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        // SAFETY: v was initialized by PMIx_Info_load
        unsafe { v.assume_init() }
    }
}
