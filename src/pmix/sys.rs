mod bindings {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(unnecessary_transmutes)]
    #![allow(clippy::missing_safety_doc)]
    #![allow(clippy::undocumented_unsafe_blocks)]
    #![allow(clippy::ptr_offset_with_cast)]

    include!(concat!(env!("OUT_DIR"), "/bindings_pmix.rs"));
}

pub use bindings::*;

impl PartialEq for pmix_proc_t {
    fn eq(&self, other: &Self) -> bool {
        self.nspace == other.nspace && self.rank == other.rank
    }
}

impl Eq for pmix_proc_t {}

impl PartialOrd for pmix_proc_t {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for pmix_proc_t {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.nspace.cmp(&other.nspace) {
            std::cmp::Ordering::Equal => self.rank.cmp(&other.rank),
            cmp => cmp,
        }
    }
}

impl std::hash::Hash for pmix_proc_t {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.nspace.hash(state);
        self.rank.hash(state);
    }
}
