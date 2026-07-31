use core::slice;
use std::ffi::CStr;
use std::fmt::Display;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::{ffi, ptr};

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

pub trait DataPtr {
    fn as_ptr(&self) -> *const ffi::c_void;
}

impl<T: ?Sized> DataPtr for &T {
    fn as_ptr(&self) -> *const ffi::c_void {
        ptr::from_ref(*self).cast()
    }
}

/// Connects a Rust type to its PMIx data-type tag.
///
/// # Safety
/// `TAG` must denote the `pmix_data_type_t` that corresponds to the Rust type,
/// and `load` and `store` must access the corresponding union member.
pub unsafe trait Tagged {
    const TAG: sys::pmix_data_type_t;
    type Data<'a>: DataPtr
    where
        Self: 'a;

    fn data(&self) -> Self::Data<'_>;

    /// # Safety
    /// Caller must ensure `src.type_ == Self::TAG`.
    unsafe fn load(src: &sys::pmix_value_t) -> &Self;

    fn tag_matches(src: &sys::pmix_value_t) -> Result<(), TagMismatch> {
        if src.type_ == Self::TAG {
            Ok(())
        } else {
            Err(TagMismatch {
                expected: Self::TAG,
                actual: src.type_,
            })
        }
    }
}

#[repr(transparent)]
pub struct Value<T: ?Sized>(sys::pmix_value_t, PhantomData<T>);

impl<T: Tagged + ?Sized> Value<T> {
    pub fn get(&self) -> &T {
        // SAFETY: tag is enforced during construction of Value<T>
        unsafe { T::load(&self.0) }
    }

    /// # Safety
    /// The value must have the correct tag for `T`, either statically known or
    /// by checking `T::tag_matches(value)`.
    pub unsafe fn load_unchecked(value: sys::pmix_value_t) -> Self {
        Self(value, PhantomData)
    }
}

impl<T: Tagged + ?Sized> From<&T> for Value<T> {
    fn from(value: &T) -> Self {
        let mut v = MaybeUninit::<sys::pmix_value_t>::uninit();
        let data = value.data();
        // SAFETY: tag is correct for data, and data is the correct pointer type
        // (that is the contract for implementing `Tagged`).
        let r = PmixStatus(unsafe { sys::PMIx_Value_load(v.as_mut_ptr(), data.as_ptr(), T::TAG) });
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

impl<T: Tagged + ?Sized> TryFrom<sys::pmix_value_t> for Value<T> {
    type Error = TagMismatch;

    fn try_from(value: sys::pmix_value_t) -> Result<Self, Self::Error> {
        T::tag_matches(&value)?;
        Ok(Self(value, PhantomData))
    }
}

macro_rules! pmix_tagged_from {
    ($T:ty, $variant:ident, $tag:expr) => {
        // SAFETY: Macro invoked with the correct type/variant/tag
        unsafe impl Tagged for $T {
            const TAG: sys::pmix_data_type_t = $tag as _;

            type Data<'a> = &'a $T;
            fn data(&self) -> Self::Data<'_> {
                self
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

            type Data<'a> = &'a $T;
            fn data(&self) -> Self::Data<'_> {
                &self.0
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

// SAFETY: Tag is correct for C-strings, and we access data.string
unsafe impl Tagged for CStr {
    const TAG: sys::pmix_data_type_t = sys::PMIX_STRING as _;

    type Data<'a> = &'a CStr;
    fn data(&self) -> Self::Data<'_> {
        self
    }

    unsafe fn load(src: &sys::pmix_value_t) -> &Self {
        // SAFETY: Type invariant is that we have the correct tag
        let ptr = unsafe { src.data.string };
        if ptr.is_null() {
            c""
        } else {
            // SAFETY: We've checked for NULL
            unsafe { CStr::from_ptr(ptr) }
        }
    }
}

/// Value that can appear as an darray element.
///
/// # Safety
/// `ELEM_TAG` must be the `pmix_data_type_t` that corresponds to the Rust type.
/// Types implementing `Element` must have the same representation as the data
/// array element representation.
pub unsafe trait Element {
    const ELEM_TAG: sys::pmix_data_type_t;

    fn array_tag_matches(src: &sys::pmix_data_array_t) -> Result<(), TagMismatch> {
        if src.type_ == Self::ELEM_TAG {
            Ok(())
        } else {
            Err(TagMismatch {
                expected: Self::ELEM_TAG,
                actual: src.type_,
            })
        }
    }
}

// SAFETY: All sized value types are valid array types (but not vice-versa, e.g.
// pmix_info_t can be an array but not a standalone value).
unsafe impl<T: Tagged> Element for T {
    const ELEM_TAG: sys::pmix_data_type_t = T::TAG;
}

pub struct DataArray<'a>(sys::pmix_data_array_t, PhantomData<&'a ()>);

impl<'a> DataPtr for DataArray<'a> {
    fn as_ptr(&self) -> *const ffi::c_void {
        ptr::from_ref(&self.0).cast()
    }
}

// SAFETY: Tag is correct for the top-level array, and recursively for inner data
unsafe impl<T: Element> Tagged for [T] {
    const TAG: sys::pmix_data_type_t = sys::PMIX_DATA_ARRAY as _;

    type Data<'a>
        = DataArray<'a>
    where
        Self: 'a;

    fn data(&self) -> Self::Data<'_> {
        DataArray(
            sys::pmix_data_array_t {
                type_: T::ELEM_TAG,
                size: self.len(),
                array: self.as_ptr() as *mut ffi::c_void,
            },
            PhantomData,
        )
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

    fn tag_matches(src: &sys::pmix_value_t) -> Result<(), TagMismatch> {
        if src.type_ == Self::TAG {
            // SAFETY: Type invariant is that we have the correct tag
            let array = unsafe { src.data.darray.as_ref() };
            array.map(|a| T::array_tag_matches(a)).unwrap_or(Ok(()))
        } else {
            Err(TagMismatch {
                expected: Self::TAG,
                actual: src.type_,
            })
        }
    }
}
