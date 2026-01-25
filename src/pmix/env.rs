#[cfg(test)]
use std::ffi::CString;
use std::{
    ffi::{CStr, OsStr},
    marker::PhantomData,
    os::unix::ffi::OsStrExt,
};

#[cfg(test)]
use std::ptr;

use super::sys;

pub struct EnvVars(*mut *mut libc::c_char);

impl EnvVars {
    pub unsafe fn from_ptr(ptr: *mut *mut libc::c_char) -> Self {
        Self(ptr)
    }

    pub fn iter(&self) -> EnvIter<'_> {
        self.into_iter()
    }

    #[cfg(test)]
    fn new(vars: &[CString]) -> Self {
        let mut ptr = ptr::null_mut();
        for var in vars {
            assert!(var.to_bytes().contains(&b'='));
            let status = unsafe { sys::PMIx_Argv_append_nosize(&mut ptr, var.as_ptr()) };
            assert_eq!(status, sys::PMIX_SUCCESS as i32);
        }
        Self(ptr)
    }
}

unsafe impl Send for EnvVars {}

impl Drop for EnvVars {
    fn drop(&mut self) {
        unsafe { sys::PMIx_Argv_free(self.0) }
    }
}

impl<'a> IntoIterator for &'a EnvVars {
    type Item = (&'a OsStr, &'a OsStr);

    type IntoIter = EnvIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        EnvIter(self.0 as *const *const u8, PhantomData)
    }
}

pub struct EnvIter<'a>(*const *const libc::c_char, PhantomData<&'a EnvVars>);

impl<'a> Iterator for EnvIter<'a> {
    type Item = (&'a OsStr, &'a OsStr);

    fn next(&mut self) -> Option<Self::Item> {
        let env_var = unsafe { *self.0 };
        self.0 = unsafe { self.0.add(1) };
        if env_var.is_null() {
            None
        } else {
            let env_var = unsafe { CStr::from_ptr(env_var) }.to_bytes();
            // Env vars populated by libpmix always contain '='
            let split = env_var.iter().position(|c| *c == b'=').unwrap();
            let k = OsStr::from_bytes(&env_var[..split]);
            let v = OsStr::from_bytes(&env_var[split + 1..]);
            Some((k, v))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_env_var() {
        let kvs = [("foo", "bar"), ("PMIX_FOO", "/tmp/foo")];
        let vars = kvs.map(|(k, v)| CString::new(format!("{}={}", k, v)).unwrap());
        let envs = EnvVars::new(&vars);
        let envs = envs.iter().collect::<Vec<_>>();
        assert_eq!(envs.len(), kvs.len());
        for (env, kv) in envs.iter().zip(kvs) {
            assert_eq!(env.0, kv.0);
            assert_eq!(env.1, kv.1);
        }
    }
}
