#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(clippy::all)]

// Include the bindgen-generated bindings
include!(concat!(env!("OUT_DIR"), "/pmix_bindings.rs"));


/// Helper to convert a Rust string to a pmix_nspace_t
pub fn str_to_nspace(s: &str) -> pmix_nspace_t {
    let mut nspace = [0u8; (PMIX_MAX_NSLEN + 1) as usize];
    let bytes = s.as_bytes();
    let len = bytes.len().min(PMIX_MAX_NSLEN as usize);
    nspace[..len].copy_from_slice(&bytes[..len]);
    nspace
}

/// Helper to convert a pmix_nspace_t to a Rust String
pub fn nspace_to_string(nspace: &pmix_nspace_t) -> String {
    // Find null terminator
    let len = nspace.iter().position(|&c| c == 0).unwrap_or(nspace.len());
    String::from_utf8_lossy(&nspace[..len]).into_owned()
}

/// Helper to convert a Rust string to a pmix_key_t
pub fn str_to_key(s: &str) -> pmix_key_t {
    let mut key = [0u8; (PMIX_MAX_KEYLEN + 1) as usize];
    let bytes = s.as_bytes();
    let len = bytes.len().min(PMIX_MAX_KEYLEN as usize);
    key[..len].copy_from_slice(&bytes[..len]);
    key
}

/// Helper to convert a pmix_key_t to a Rust String
pub fn key_to_string(key: &pmix_key_t) -> String {
    // Find null terminator
    let len = key.iter().position(|&c| c == 0).unwrap_or(key.len());
    String::from_utf8_lossy(&key[..len]).into_owned()
}

/// Create a pmix_proc_t from namespace and rank
pub fn make_proc(nspace: &str, rank: pmix_rank_t) -> pmix_proc_t {
    pmix_proc_t {
        nspace: str_to_nspace(nspace),
        rank,
    }
}

/// Check if a PMIx status is successful
pub fn is_success(status: pmix_status_t) -> bool {
    status == PMIX_SUCCESS as i32
}

/// Convert PMIx status to a human-readable string
pub fn status_string(status: pmix_status_t) -> &'static str {
    match status {
        s if s == PMIX_SUCCESS as i32 => "PMIX_SUCCESS",
        s if s == PMIX_ERR_NOT_SUPPORTED as i32 => "PMIX_ERR_NOT_SUPPORTED",
        s if s == PMIX_ERR_NOT_FOUND as i32 => "PMIX_ERR_NOT_FOUND",
        s if s == PMIX_ERR_TIMEOUT as i32 => "PMIX_ERR_TIMEOUT",
        s if s == PMIX_ERR_BAD_PARAM as i32 => "PMIX_ERR_BAD_PARAM",
        s if s == PMIX_ERR_INIT as i32 => "PMIX_ERR_INIT",
        s if s == PMIX_ERR_NOMEM as i32 => "PMIX_ERR_NOMEM",
        _ => "PMIX_ERR_UNKNOWN",
    }
}
