use std::{
    ffi::CStr,
    mem::MaybeUninit,
    ptr,
    sync::{Mutex, MutexGuard, PoisonError},
};

use super::sys;

static CLIENT_LOCK: Mutex<()> = Mutex::new(());

pub struct Client {
    // I'm not sure what PMIx client functions are thread-safe, so just lock the
    // whole thing for now.
    _guard: MutexGuard<'static, ()>,
    proc: sys::pmix_proc_t,
}

#[derive(Clone, Copy, Debug)]
pub struct Session(u32);

#[derive(Clone, Copy, Debug)]
pub struct Job<'a>(&'a CStr, Option<Session>);

impl Client {
    pub fn init(infos: &[sys::pmix_info_t]) -> Result<Client, PoisonError<MutexGuard<'_, ()>>> {
        let guard = CLIENT_LOCK.lock()?;
        let mut proc = MaybeUninit::<sys::pmix_proc_t>::uninit();
        let status = unsafe {
            sys::PMIx_Init(
                proc.as_mut_ptr(),
                infos.as_ptr() as *mut sys::pmix_info_t,
                infos.len(),
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as i32);
        let proc = unsafe { proc.assume_init() };

        Ok(Self {
            _guard: guard,
            proc,
        })
    }

    pub fn rank(&self) -> u32 {
        self.proc.rank
    }

    pub fn namespace(&self) -> &CStr {
        let namespace = self.proc.nspace;
        let namespace =
            unsafe { std::slice::from_raw_parts(namespace.as_ptr() as *const u8, namespace.len()) };

        // PMIx initializes the namespace, so it is guaranteed to be valid
        CStr::from_bytes_until_nul(namespace).unwrap()
    }

    fn get(
        proc: Option<&sys::pmix_proc_t>,
        #[allow(unused_mut)] mut infos: Vec<sys::pmix_info_t>,
        key: &CStr,
    ) -> sys::pmix_value_t {
        // We should use PMIX_GET_STATIC_VALUES, but this does not work. See
        // github.com/openpmix/openpmix#3782.
        let mut val_p = MaybeUninit::<*mut sys::pmix_value_t>::uninit();
        let status = unsafe {
            sys::PMIx_Get(
                proc.map_or(ptr::null(), |p| p),
                key.as_ptr(),
                infos.as_ptr(),
                infos.len(),
                val_p.as_mut_ptr(),
            )
        };
        assert_eq!(status, sys::PMIX_SUCCESS as i32);
        let val_p = unsafe { val_p.assume_init() };
        let val = unsafe { val_p.read() };

        // Mark the source as PMIX_UNDEF, so the data we've moved into val is not free'd.
        unsafe {
            (*val_p).type_ = sys::PMIX_UNDEF as u16;
            sys::PMIx_Value_free(val_p, 1);
        }

        val
    }

    pub fn get_session(&self, session: Option<Session>, key: &CStr) -> sys::pmix_value_t {
        let mut infos = Vec::with_capacity(3);
        infos.push((sys::PMIX_SESSION_INFO, true).into());
        if let Some(Session(id)) = session {
            infos.push((sys::PMIX_SESSION_ID, id).into());
        }

        Self::get(None, infos, key)
    }

    pub fn get_job(&self, job: Option<Job>, key: &CStr) -> sys::pmix_value_t {
        let mut infos = Vec::with_capacity(4);
        infos.push((sys::PMIX_JOB_INFO, true).into());
        if let Some(Job(_, Some(Session(id)))) = job {
            infos.push((sys::PMIX_SESSION_ID, id).into())
        }
        if let Some(Job(id, _)) = job {
            infos.push((sys::PMIX_JOBID, id).into())
        }

        let proc = sys::pmix_proc_t {
            rank: sys::PMIX_RANK_WILDCARD,
            ..self.proc
        };

        Self::get(Some(&proc), infos, key)
    }
}
