use std::ffi;
use std::mem::MaybeUninit;

use crate::pmix::value::DataPtr;

use super::sys;
use super::value::{self, Element, PmixStatus, Tagged};

/// # SAFETY
/// `KEY` must be the correct PMIx info key for the `Value` type.
pub unsafe trait Key {
    const KEY: &ffi::CStr;

    type Value: Tagged + ?Sized;

    fn info(value: &Self::Value) -> sys::pmix_info_t {
        let mut v = MaybeUninit::<sys::pmix_info_t>::uninit();
        let data = value.data();
        // SAFETY: tag is correct for data, and data is the correct pointer type
        // (that is the contract for implementing `Tagged`). Key is correct for
        // the value type (that is the contract for implementing `Key`).
        let r = PmixStatus(unsafe {
            sys::PMIx_Info_load(
                v.as_mut_ptr(),
                Self::KEY.as_ptr(),
                data.as_ptr(),
                Self::Value::TAG,
            )
        });
        assert!(r.check().is_ok());
        // SAFETY: initialized with `K::store`, and return code checked
        unsafe { v.assume_init() }
    }
}

// SAFETY: Info elements are valid arrays, and this is type-erased so we don't
// need a recursive check.
unsafe impl Element for sys::pmix_info_t {
    const ELEM_TAG: sys::pmix_data_type_t = sys::PMIX_INFO as _;
}

macro_rules! pmix_info_key_from {
    ($S:ident, $T:ty, $tag:expr) => {
        pub struct $S();
        // SAFETY: Macro invoked with the correct type/tag
        unsafe impl Key for $S {
            const KEY: &ffi::CStr = $tag;
            type Value = $T;
        }
    };
}

pmix_info_key_from!(SessionInfo, bool, sys::PMIX_SESSION_INFO);
pmix_info_key_from!(SessionId, u32, sys::PMIX_SESSION_ID);
pmix_info_key_from!(JobInfo, bool, sys::PMIX_JOB_INFO);

pmix_info_key_from!(JobSize, u32, sys::PMIX_JOB_SIZE);
pmix_info_key_from!(Rank, value::Rank, sys::PMIX_RANK);
pmix_info_key_from!(LocalRank, u16, sys::PMIX_LOCAL_RANK);
pmix_info_key_from!(NodeId, u32, sys::PMIX_NODEID);
pmix_info_key_from!(Hostname, ffi::CStr, sys::PMIX_HOSTNAME);
pmix_info_key_from!(NodeInfo, [sys::pmix_info_t], sys::PMIX_NODE_INFO_ARRAY);
pmix_info_key_from!(ProcInfo, [sys::pmix_info_t], sys::PMIX_PROC_INFO_ARRAY);
pmix_info_key_from!(ServerTmpdir, ffi::CStr, sys::PMIX_SERVER_TMPDIR);
pmix_info_key_from!(SystemTmpdir, ffi::CStr, sys::PMIX_SYSTEM_TMPDIR);
pmix_info_key_from!(ServerSystemSupport, bool, sys::PMIX_SERVER_SYSTEM_SUPPORT);

#[cfg(test)]
mod test {
    #![allow(clippy::undocumented_unsafe_blocks)]
    #![allow(clippy::unwrap_used)]

    use super::*;

    pub struct TestKey();
    unsafe impl Key for TestKey {
        const KEY: &ffi::CStr = c"test.key";
        type Value = [u32];
    }

    fn into_value(info: sys::pmix_info_t) -> sys::pmix_value_t {
        let value = unsafe { std::ptr::read(&info.value) };
        std::mem::forget(info);
        value
    }

    fn key(info: &sys::pmix_info_t) -> &ffi::CStr {
        unsafe { ffi::CStr::from_ptr(info.key.as_ptr()) }
    }

    #[test]
    fn test_key() {
        let info = TestKey::info(&[]);
        assert_eq!(key(&info), TestKey::KEY);
    }

    #[test]
    fn test_round_trip_scalar() {
        let value = into_value(JobSize::info(&42));
        assert_eq!(value::Value::<u32>::try_from(value).unwrap().get(), &42);
    }

    #[test]
    fn test_round_trip_array() {
        let value = into_value(TestKey::info(&[1, 2, 3]));
        assert_eq!(
            value::Value::<[u32]>::try_from(value).unwrap().get(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn test_round_trip_empty_array() {
        let value = into_value(TestKey::info(&[]));
        assert_eq!(
            value::Value::<[u32]>::try_from(value).unwrap().get(),
            &[0; 0]
        );
    }

    #[test]
    fn test_round_trip_info_array() {
        let value = into_value(NodeInfo::info(&[NodeId::info(&7)]));
        let infos = value::Value::<[sys::pmix_info_t]>::try_from(value).unwrap();
        let infos = infos.get();
        assert_eq!(infos.len(), 1);
        assert_eq!(key(&infos[0]), NodeId::KEY);

        assert!(u32::tag_matches(&infos[0].value).is_ok());
        // SAFETY: tag checked above
        assert_eq!(unsafe { u32::load(&infos[0].value) }, &7);
    }

    #[test]
    fn test_tag_mismatch() {
        let value = into_value(JobSize::info(&42));
        assert!(value::Value::<u16>::try_from(value).is_err());

        let value = into_value(JobSize::info(&42));
        assert!(value::Value::<[u32]>::try_from(value).is_err());
    }
}
