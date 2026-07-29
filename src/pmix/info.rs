use std::ffi::{CStr, c_void};
use std::mem::MaybeUninit;

use super::sys;
use super::value::{self, PmixStatus, PmixStr, Tagged};

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
    ($S:ident, $T:ty, $tag:expr) => {
        pub struct $S();
        // SAFETY: Macro invoked with the correct type/tag
        unsafe impl Key for $S {
            const KEY: &CStr = $tag;
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
pmix_info_key_from!(UniverseSize, u32, sys::PMIX_UNIV_SIZE);
pmix_info_key_from!(Hostname, PmixStr, sys::PMIX_HOSTNAME);
pmix_info_key_from!(NodeMap, PmixStr, sys::PMIX_NODE_MAP);
pmix_info_key_from!(ProcMap, PmixStr, sys::PMIX_PROC_MAP);
pmix_info_key_from!(ServerTmpdir, PmixStr, sys::PMIX_SERVER_TMPDIR);
pmix_info_key_from!(SystemTmpdir, PmixStr, sys::PMIX_SYSTEM_TMPDIR);
pmix_info_key_from!(ServerSystemSupport, bool, sys::PMIX_SERVER_SYSTEM_SUPPORT);
