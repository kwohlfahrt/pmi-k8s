use std::{ffi, fmt::Debug, marker::PhantomData, os::unix::ffi::OsStrExt};

#[cfg(test)]
use std::ptr;

use super::sys;

pub struct EnvVars(*mut *mut ffi::c_char);

impl EnvVars {
    /// # Safety
    ///
    /// `ptr` must be an `argv`-style array of `char*`, terminated by a `NULL`
    /// pointer.
    pub unsafe fn from_ptr(ptr: *mut *mut ffi::c_char) -> Self {
        Self(ptr)
    }

    pub fn iter(&self) -> EnvIter<'_> {
        self.into_iter()
    }

    #[cfg(test)]
    fn new(vars: &[ffi::CString]) -> Self {
        let mut ptr = ptr::null_mut();
        for var in vars {
            assert!(var.to_bytes().contains(&b'='));
            // SAFETY: `ptr` may be null, or an existing `argv`-style array. It
            // is created as `NULL` above, and only modified here. `var` must be
            // a null-terminated `char*`. Enforced by `ffi::CString`.
            let status = unsafe { sys::PMIx_Argv_append_nosize(&mut ptr, var.as_ptr()) };
            assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
        }
        Self(ptr)
    }
}

impl Debug for EnvVars {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self).finish()
    }
}

// SAFETY: EnvVars only contains pointers to text, which can safely be moved.
unsafe impl Send for EnvVars {}

impl Drop for EnvVars {
    fn drop(&mut self) {
        // SAFETY: This is the destructor for `argv`-style arrays.
        unsafe { sys::PMIx_Argv_free(self.0) }
    }
}

impl<'a> IntoIterator for &'a EnvVars {
    type Item = (&'a ffi::OsStr, &'a ffi::OsStr);

    type IntoIter = EnvIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        EnvIter(self.0 as *const *const ffi::c_char, PhantomData)
    }
}

pub struct EnvIter<'a>(*const *const ffi::c_char, PhantomData<&'a EnvVars>);

impl<'a> Iterator for EnvIter<'a> {
    type Item = (&'a ffi::OsStr, &'a ffi::OsStr);

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: We checked on the previous iteration, that this is in-bounds.
        let env_var = unsafe { *self.0 };
        if env_var.is_null() {
            None
        } else {
            // SAFETY: The current value is not `NULL``, so the array must
            // contain at least one more element.
            self.0 = unsafe { self.0.add(1) };
            // SAFETY: env vars are either provided by libpmix (in which case we
            // assume they are valid C strings), or from `ffi::CString` in
            // `EnvVars::new()`.
            let env_var = unsafe { ffi::CStr::from_ptr(env_var) }.to_bytes();
            #[allow(
                clippy::unwrap_used,
                reason = "Env vars populated by libpmix always contain '='"
            )]
            let split = env_var.iter().position(|c| *c == b'=').unwrap();
            let k = ffi::OsStr::from_bytes(&env_var[..split]);
            let v = ffi::OsStr::from_bytes(&env_var[split + 1..]);
            Some((k, v))
        }
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_env_var() {
        let kvs = [("foo", "bar"), ("PMIX_FOO", "/tmp/foo")];
        let vars = kvs.map(|(k, v)| ffi::CString::new(format!("{}={}", k, v)).unwrap());
        let envs = EnvVars::new(&vars);
        let envs = envs.iter().collect::<Vec<_>>();
        assert_eq!(envs.len(), kvs.len());
        for (env, kv) in envs.iter().zip(kvs) {
            assert_eq!(env.0, kv.0);
            assert_eq!(env.1, kv.1);
        }
    }
}
