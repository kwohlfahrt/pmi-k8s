use std::{ffi::CStr, mem::MaybeUninit, ptr};

use crate::pmix::value;

use super::info::{self, Key};

use super::globals;
use super::sys;
use super::value::{PmixError, PmixStatus};

pub struct Client {
    proc: sys::pmix_proc_t,
}

#[derive(Clone, Copy, Debug)]
pub struct Session(u32);

#[derive(Clone, Copy, Debug)]
pub struct Job(sys::pmix_nspace_t, Option<Session>);

#[derive(Clone, Copy, Debug)]
pub struct Proc(u32, Option<Job>);

impl Client {
    pub fn init(infos: &[sys::pmix_info_t]) -> Result<Client, globals::InitError> {
        #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
        let mut guard = globals::PMIX_STATE.write().unwrap();

        if guard.is_some() {
            Err(globals::InitError::AlreadyInitialized)?;
        }

        let mut proc = MaybeUninit::<sys::pmix_proc_t>::uninit();
        // SAFETY: `PMIx_init` can be called multiple times, as long as there
        // are matching calls to `PMIx_Finalize`.
        PmixStatus(unsafe {
            sys::PMIx_Init(
                proc.as_mut_ptr(),
                infos.as_ptr() as *mut sys::pmix_info_t,
                infos.len(),
            )
        })
        .check()?;
        // SAFETY: `proc` is initialized by `PMIx_Init`
        let proc = unsafe { proc.assume_init() };
        *guard = Some(globals::State::Client);

        Ok(Self { proc })
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

    fn get<K>(
        proc: Option<&sys::pmix_proc_t>,
        infos: Vec<sys::pmix_info_t>,
    ) -> Result<value::Value<K::Value>, PmixError>
    where
        K: info::Key<Value: Sized>,
    {
        // We should use PMIX_GET_STATIC_VALUES, but this does not work. See
        // github.com/openpmix/openpmix#3782. Once this is resolved, the dance
        // to free `val_p` below is no longer necessary.
        let mut val_p = MaybeUninit::<*mut sys::pmix_value_t>::uninit();

        // SAFETY: `key` is a valid C string, `val` is a single-element pointer.
        PmixStatus(unsafe {
            sys::PMIx_Get(
                proc.map_or(ptr::null(), |p| p),
                K::KEY.as_ptr(),
                infos.as_ptr(),
                infos.len(),
                val_p.as_mut_ptr(),
            )
        })
        .check()?;

        // SAFETY: `val_p` is initialized by the call to PMIx_Get above. We now
        // own the pointed-to data, so it is free'd with `PMIx_Value_free`.
        // However, the value object we return also points to the same interior
        // data, so we set the type of `val_p` to `PMIX_UNDEF`, to move
        // ownership of the interior data to the returned `sys::pmix_value_t`.
        let val = unsafe {
            let val_p = val_p.assume_init();
            let val = val_p.read();

            (*val_p).type_ = sys::PMIX_UNDEF as u16;
            sys::PMIx_Value_free(val_p, 1);
            val
        };

        // SAFETY: The key's corresponding value tyep is statically known.
        Ok(unsafe { value::Value::<K::Value>::load_unchecked(val) })
    }

    pub fn get_session<K>(
        &self,
        session: Option<Session>,
    ) -> Result<value::Value<K::Value>, PmixError>
    where
        K: info::Key<Value: Sized>,
    {
        let mut infos = Vec::with_capacity(3);
        infos.push(info::SessionInfo::info(&true));
        if let Some(Session(id)) = session {
            infos.push(info::SessionId::info(&id));
        }

        Self::get::<K>(None, infos)
    }

    pub fn get_job<K>(&self, job: Option<Job>) -> Result<value::Value<K::Value>, PmixError>
    where
        K: info::Key<Value: Sized>,
    {
        let mut infos = Vec::with_capacity(3);
        infos.push(info::JobInfo::info(&true));
        if let Some(Job(_, Some(Session(id)))) = job {
            infos.push(info::SessionId::info(&id))
        }

        let proc = sys::pmix_proc_t {
            nspace: job.map_or(self.proc.nspace, |j| j.0),
            rank: sys::PMIX_RANK_WILDCARD,
        };

        Self::get::<K>(Some(&proc), infos)
    }

    pub fn get_proc<K>(&self, proc: Option<Proc>) -> Result<value::Value<K::Value>, PmixError>
    where
        K: info::Key<Value: Sized>,
    {
        let mut infos = Vec::with_capacity(2);
        if let Some(Proc(_, Some(Job(_, Some(Session(id)))))) = proc {
            infos.push(info::SessionId::info(&id))
        }

        let proc = sys::pmix_proc_t {
            nspace: proc.and_then(|p| p.1).map_or(self.proc.nspace, |j| j.0),
            rank: proc.map_or(self.proc.rank, |p| p.0),
        };

        Self::get::<K>(Some(&proc), infos)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // SAFETY: PMIx_Finalize must match a call to PMIx_Init.
        let status = unsafe { sys::PMIx_Finalize(ptr::null(), 0) };
        assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);

        #[allow(clippy::unwrap_used, reason = "no asserts poison the global state")]
        let mut guard = globals::PMIX_STATE.write().unwrap();
        drop(guard.take());
    }
}
