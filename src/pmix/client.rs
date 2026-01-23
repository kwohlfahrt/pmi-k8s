use std::sync::{Mutex, MutexGuard, PoisonError};

use super::sys;

static CLIENT_LOCK: Mutex<()> = Mutex::new(());

pub struct Client {
    // I'm not sure what PMIx client functions are thread-safe, so just lock the
    // whole thing for now.
    _guard: MutexGuard<'static, ()>,
}

impl Client {
    pub fn init(infos: &[sys::pmix_info_t]) -> Result<Client, PoisonError<MutexGuard<'_, ()>>> {
        let c = Self {
            _guard: CLIENT_LOCK.lock()?,
        };
        let status = unsafe {
            sys::PMIx_Init(
                std::ptr::null_mut(),
                infos.as_ptr() as *mut sys::pmix_info_t,
                infos.len(),
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as i32);
        Ok(c)
    }
}
