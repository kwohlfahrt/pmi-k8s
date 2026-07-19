impl PartialEq for super::pmix_proc_t {
    fn eq(&self, other: &Self) -> bool {
        self.nspace == other.nspace && self.rank == other.rank
    }
}

impl Eq for super::pmix_proc_t {}

impl PartialOrd for super::pmix_proc_t {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for super::pmix_proc_t {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.nspace.cmp(&other.nspace) {
            std::cmp::Ordering::Equal => self.rank.cmp(&other.rank),
            cmp => cmp,
        }
    }
}

impl std::hash::Hash for super::pmix_proc_t {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.nspace.hash(state);
        self.rank.hash(state);
    }
}
