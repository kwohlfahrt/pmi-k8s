use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use thiserror::Error;
use tracing::{debug, info, warn};

use super::protocol::{send_message, CoordMessage};
use crate::k8s::pods::PeerPod;
use crate::kv_store::KvStore;
use crate::pmix::server::FenceCallback;

/// A fence request from the local PMIx client
#[derive(Debug, Clone)]
pub struct FenceRequest {
    /// Participating processes: (nspace, rank)
    pub participants: Vec<(String, u32)>,
}

/// State of an ongoing fence operation
struct FenceState {
    /// Number of participants expected
    expected_count: usize,
    /// Data received from each rank
    received_data: HashMap<u32, Bytes>,
    /// Callback to invoke when fence completes
    callback: Option<FenceCallback>,
}

/// Coordinator for distributed fence operations
pub struct FenceCoordinator {
    /// Our rank
    local_rank: u32,
    /// World size
    world_size: u32,
    /// Peer pod addresses by rank
    peers: HashMap<u32, String>,
    /// KV store for caching modex data
    kv_store: Arc<KvStore>,
    /// Active fence operations
    active_fences: DashMap<u64, FenceState>,
    /// Counter for fence IDs
    fence_counter: AtomicU64,
}

impl FenceCoordinator {
    pub fn new(
        local_rank: u32,
        world_size: u32,
        peers: Vec<PeerPod>,
        kv_store: Arc<KvStore>,
    ) -> Self {
        let peer_addrs: HashMap<u32, String> = peers
            .into_iter()
            .map(|p| (p.rank, p.coord_addr()))
            .collect();

        Self {
            local_rank,
            world_size,
            peers: peer_addrs,
            kv_store,
            active_fences: DashMap::new(),
            fence_counter: AtomicU64::new(0),
        }
    }

    /// Start a new fence operation
    ///
    /// This is called when the local PMIx client initiates a fence.
    pub async fn start_fence(
        &self,
        _request: FenceRequest,
        local_data: Vec<u8>,
        callback: FenceCallback,
        nspace: &str,
    ) -> Result<(), FenceError> {
        let fence_id = self.fence_counter.fetch_add(1, Ordering::SeqCst);

        info!(
            fence_id,
            local_rank = self.local_rank,
            data_len = local_data.len(),
            "Starting fence"
        );

        // For a global fence, we expect data from all ranks
        let expected_count = self.world_size as usize;

        // Store local data in KV store
        self.kv_store
            .put_modex_data(nspace, self.local_rank, local_data.clone());

        // Initialize fence state
        let mut state = FenceState {
            expected_count,
            received_data: HashMap::new(),
            callback: Some(callback),
        };

        // Add our own data
        state
            .received_data
            .insert(self.local_rank, Bytes::from(local_data.clone()));

        // If we're the only participant, complete immediately
        if expected_count == 1 {
            self.complete_fence(fence_id, state, nspace);
            return Ok(());
        }

        // Store state
        self.active_fences.insert(fence_id, state);

        // Send our data to all peers
        let msg = CoordMessage::FenceData {
            fence_id,
            rank: self.local_rank,
            data: Bytes::from(local_data),
        };

        for (&peer_rank, addr) in &self.peers {
            if peer_rank != self.local_rank {
                let addr = addr.clone();
                let msg = msg.clone();
                tokio::spawn(async move {
                    if let Err(e) = send_message(&addr, &msg).await {
                        warn!(peer_rank, error = %e, "Failed to send fence data to peer");
                    }
                });
            }
        }

        Ok(())
    }

    /// Handle fence data received from a peer
    pub fn handle_fence_data(&self, fence_id: u64, rank: u32, data: Bytes, nspace: &str) {
        debug!(fence_id, rank, data_len = data.len(), "Received fence data");

        // Store in KV store
        self.kv_store.put_modex_data(nspace, rank, data.to_vec());

        // Update fence state
        if let Some(mut state) = self.active_fences.get_mut(&fence_id) {
            state.received_data.insert(rank, data);

            // Check if fence is complete
            if state.received_data.len() >= state.expected_count {
                // Remove from active fences and complete
                drop(state);
                if let Some((_, state)) = self.active_fences.remove(&fence_id) {
                    self.complete_fence(fence_id, state, nspace);
                }
            }
        } else {
            // Fence not yet started locally - store data for later
            // This handles the case where remote data arrives before local fence starts
            debug!(fence_id, rank, "Received fence data before local fence started");
        }
    }

    /// Complete a fence operation
    fn complete_fence(&self, fence_id: u64, state: FenceState, nspace: &str) {
        info!(
            fence_id,
            num_participants = state.received_data.len(),
            "Fence complete"
        );

        // Build the combined data blob to return
        // Format: repeated (rank:u32, len:u32, data:bytes)
        let mut combined = Vec::new();
        for (rank, data) in &state.received_data {
            combined.extend_from_slice(&rank.to_le_bytes());
            combined.extend_from_slice(&(data.len() as u32).to_le_bytes());
            combined.extend_from_slice(data);
        }

        // Store all data in KV store
        let bulk_data: Vec<(u32, Vec<u8>)> = state
            .received_data
            .iter()
            .map(|(r, d)| (*r, d.to_vec()))
            .collect();
        self.kv_store.put_bulk_modex_data(nspace, &bulk_data);

        // Invoke callback
        if let Some(callback) = state.callback {
            callback.complete(
                crate::pmix::bindings::PMIX_SUCCESS as i32,
                &combined,
            );
        }
    }

    /// Get the number of active fence operations
    pub fn active_fence_count(&self) -> usize {
        self.active_fences.len()
    }
}

#[derive(Debug, Error)]
pub enum FenceError {
    #[error("Protocol error: {0}")]
    Protocol(#[from] super::protocol::ProtocolError),
    #[error("Fence timeout")]
    Timeout,
}
