use dashmap::DashMap;
use std::sync::Arc;

/// Key for the KV store: (namespace, rank, key)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct KvKey {
    pub nspace: String,
    pub rank: u32,
    pub key: String,
}

impl KvKey {
    pub fn new(nspace: impl Into<String>, rank: u32, key: impl Into<String>) -> Self {
        Self {
            nspace: nspace.into(),
            rank,
            key: key.into(),
        }
    }

    /// Create a key for modex data (the blob exchanged during fence)
    pub fn modex(nspace: impl Into<String>, rank: u32) -> Self {
        Self::new(nspace, rank, "__modex__")
    }
}

/// Thread-safe key-value store for PMIx data
#[derive(Debug, Default)]
pub struct KvStore {
    /// Main data store
    data: DashMap<KvKey, Vec<u8>>,
    /// Store for modex blobs (data exchanged during fence)
    modex_data: DashMap<(String, u32), Vec<u8>>,
}

impl KvStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            data: DashMap::new(),
            modex_data: DashMap::new(),
        })
    }

    /// Store a value
    pub fn put(&self, key: KvKey, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    /// Get a value
    pub fn get(&self, key: &KvKey) -> Option<Vec<u8>> {
        self.data.get(key).map(|v| v.clone())
    }

    /// Remove a value
    pub fn remove(&self, key: &KvKey) -> Option<Vec<u8>> {
        self.data.remove(key).map(|(_, v)| v)
    }

    /// Store modex data for a process
    pub fn put_modex_data(&self, nspace: &str, rank: u32, data: Vec<u8>) {
        self.modex_data.insert((nspace.to_string(), rank), data);
    }

    /// Get modex data for a process
    pub fn get_modex_data(&self, nspace: &str, rank: u32) -> Option<Vec<u8>> {
        self.modex_data
            .get(&(nspace.to_string(), rank))
            .map(|v| v.clone())
    }

    /// Store modex data from multiple ranks (during fence completion)
    pub fn put_bulk_modex_data(&self, nspace: &str, data: &[(u32, Vec<u8>)]) {
        for (rank, blob) in data {
            self.modex_data
                .insert((nspace.to_string(), *rank), blob.clone());
        }
    }

    /// Get all local data for a namespace as a serialized blob
    pub fn get_local_data_blob(&self, nspace: &str, local_rank: u32) -> Vec<u8> {
        self.modex_data
            .get(&(nspace.to_string(), local_rank))
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Clear all data for a namespace
    pub fn clear_namespace(&self, nspace: &str) {
        self.data.retain(|k, _| k.nspace != nspace);
        self.modex_data.retain(|(ns, _), _| ns != nspace);
    }

    /// Get statistics
    pub fn stats(&self) -> KvStoreStats {
        KvStoreStats {
            num_keys: self.data.len(),
            num_modex_entries: self.modex_data.len(),
        }
    }
}

#[derive(Debug)]
pub struct KvStoreStats {
    pub num_keys: usize,
    pub num_modex_entries: usize,
}
