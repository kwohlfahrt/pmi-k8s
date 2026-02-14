use std::marker::PhantomData;
use std::{ffi::CStr, mem::MaybeUninit, ptr};

use super::globals;
use super::sys;

pub struct Client {
    proc: sys::pmix_proc_t,
    // I'm not sure what PMIx functions are thread-safe, so mark the client as
    // !Sync. Client::init enforces that only one is live at a time.
    _marker: globals::Unsync,
}

#[derive(Clone, Copy, Debug)]
pub struct Session(u32);

#[derive(Clone, Copy, Debug)]
pub struct Job(sys::pmix_nspace_t, Option<Session>);

#[derive(Clone, Copy, Debug)]
pub struct Proc(u32, Option<Job>);

impl Client {
    pub fn init(infos: &[sys::pmix_info_t]) -> Result<Client, globals::AlreadyInitialized> {
        let mut guard = globals::PMIX_STATE.write().unwrap();
        if guard.is_some() {
            return Err(globals::AlreadyInitialized());
        }

        let mut proc = MaybeUninit::<sys::pmix_proc_t>::uninit();
        let status = unsafe {
            sys::PMIx_Init(
                proc.as_mut_ptr(),
                infos.as_ptr() as *mut sys::pmix_info_t,
                infos.len(),
            )
        };
        // FIXME: Don't poison the mutes on init error
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        // SAFETY: `proc` is initialized by `PMIx_Init`
        let proc = unsafe { proc.assume_init() };
        *guard = Some(globals::State::Client);

        Ok(Self {
            proc,
            _marker: globals::Unsync(PhantomData),
        })
    }

    pub fn rank(&self) -> u32 {
        self.proc.rank
    }

    pub fn namespace(&self) -> &CStr {
        let namespace = super::char_to_u8(&self.proc.nspace);

        #[allow(
            clippy::unwrap_used,
            reason = "Namespace is initialized by PMIx as a C string"
        )]
        CStr::from_bytes_until_nul(namespace).unwrap()
    }

    fn get(
        proc: Option<&sys::pmix_proc_t>,
        infos: Vec<sys::pmix_info_t>,
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
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
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
        let mut infos = Vec::with_capacity(3);
        infos.push((sys::PMIX_JOB_INFO, true).into());
        if let Some(Job(_, Some(Session(id)))) = job {
            infos.push((sys::PMIX_SESSION_ID, id).into())
        }

        let proc = sys::pmix_proc_t {
            nspace: job.map_or(self.proc.nspace, |j| j.0),
            rank: sys::PMIX_RANK_WILDCARD,
        };

        Self::get(Some(&proc), infos, key)
    }

    pub fn get_proc(&self, proc: Option<Proc>, key: &CStr) -> sys::pmix_value_t {
        let mut infos = Vec::with_capacity(2);
        if let Some(Proc(_, Some(Job(_, Some(Session(id)))))) = proc {
            infos.push((sys::PMIX_SESSION_ID, id).into())
        }

        let proc = sys::pmix_proc_t {
            nspace: proc.and_then(|p| p.1).map_or(self.proc.nspace, |j| j.0),
            rank: proc.map_or(self.proc.rank, |p| p.0),
        };

        Self::get(Some(&proc), infos, key)
    }
}
