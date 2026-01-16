use std::collections::HashMap;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, ListParams},
    runtime::watcher::{self, Event},
    Client,
};
use thiserror::Error;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use super::env::PodIdentity;

/// Information about a peer pod
#[derive(Debug, Clone)]
pub struct PeerPod {
    /// Pod name
    pub name: String,
    /// Pod IP address
    pub ip: String,
    /// MPI rank (from completion index)
    pub rank: u32,
    /// Coordination port
    pub coord_port: u16,
}

impl PeerPod {
    /// Get the coordination address
    pub fn coord_addr(&self) -> String {
        format!("{}:{}", self.ip, self.coord_port)
    }
}

/// Pod discovery service
pub struct PodDiscovery {
    client: Client,
    identity: PodIdentity,
}

impl PodDiscovery {
    /// Create a new pod discovery service
    pub async fn new(identity: PodIdentity) -> Result<Self, DiscoveryError> {
        let client = Client::try_default()
            .await
            .map_err(DiscoveryError::KubeClient)?;

        Ok(Self { client, identity })
    }

    /// Discover all peer pods in the same job
    ///
    /// Waits until all expected pods are ready or timeout is reached.
    pub async fn discover_peers(&self, discovery_timeout: Duration) -> Result<Vec<PeerPod>, DiscoveryError> {
        let pods_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.identity.namespace);

        // Label selector for pods in the same job
        let label_selector = format!("job-name={}", self.identity.job_name);
        let lp = ListParams::default().labels(&label_selector);

        info!(
            job_name = self.identity.job_name,
            world_size = self.identity.world_size,
            "Discovering peer pods"
        );

        // Wait for all pods to be ready
        let peers = timeout(discovery_timeout, self.wait_for_pods(&pods_api, &lp))
            .await
            .map_err(|_| DiscoveryError::Timeout)??;

        info!(num_peers = peers.len(), "Discovered all peer pods");
        Ok(peers)
    }

    /// Wait for all expected pods to be ready
    async fn wait_for_pods(
        &self,
        pods_api: &Api<Pod>,
        lp: &ListParams,
    ) -> Result<Vec<PeerPod>, DiscoveryError> {
        let mut ready_pods: HashMap<String, PeerPod> = HashMap::new();
        let expected_count = self.identity.world_size as usize;

        // Start watching pods
        let mut stream = watcher::watcher(pods_api.clone(), watcher::Config::default().labels(&lp.label_selector.clone().unwrap_or_default())).boxed();

        // Also do an initial list to get current state
        let initial_pods = pods_api
            .list(lp)
            .await
            .map_err(DiscoveryError::KubeApi)?;

        for pod in initial_pods {
            if let Some(peer) = self.pod_to_peer(&pod) {
                debug!(name = peer.name, rank = peer.rank, ip = peer.ip, "Found peer pod");
                ready_pods.insert(peer.name.clone(), peer);
            }
        }

        if ready_pods.len() >= expected_count {
            return Ok(ready_pods.into_values().collect());
        }

        // Watch for changes until we have all pods
        while let Some(event) = stream.next().await {
            match event {
                Ok(Event::Apply(pod)) | Ok(Event::InitApply(pod)) => {
                    if let Some(peer) = self.pod_to_peer(&pod) {
                        debug!(name = peer.name, rank = peer.rank, ip = peer.ip, "Peer pod ready");
                        ready_pods.insert(peer.name.clone(), peer);

                        if ready_pods.len() >= expected_count {
                            return Ok(ready_pods.into_values().collect());
                        }
                    }
                }
                Ok(Event::Delete(pod)) => {
                    if let Some(name) = pod.metadata.name {
                        warn!(name, "Peer pod deleted");
                        ready_pods.remove(&name);
                    }
                }
                Ok(Event::Init) | Ok(Event::InitDone) => {}
                Err(e) => {
                    warn!(error = %e, "Watch error");
                }
            }
        }

        Err(DiscoveryError::WatchEnded)
    }

    /// Convert a Kubernetes Pod to a PeerPod if it's ready
    fn pod_to_peer(&self, pod: &Pod) -> Option<PeerPod> {
        let metadata = &pod.metadata;
        let name = metadata.name.as_ref()?;
        let labels = metadata.labels.as_ref()?;

        // Get completion index from label
        let completion_index: u32 = labels
            .get("batch.kubernetes.io/job-completion-index")
            .or_else(|| labels.get("job-completion-index"))?
            .parse()
            .ok()?;

        // Check if pod is ready
        let status = pod.status.as_ref()?;
        let phase = status.phase.as_ref()?;
        if phase != "Running" {
            return None;
        }

        // Check conditions for Ready
        let conditions = status.conditions.as_ref()?;
        let is_ready = conditions
            .iter()
            .any(|c| c.type_ == "Ready" && c.status == "True");
        if !is_ready {
            return None;
        }

        // Get pod IP
        let pod_ip = status.pod_ip.as_ref()?;

        Some(PeerPod {
            name: name.clone(),
            ip: pod_ip.clone(),
            rank: completion_index,
            coord_port: self.identity.coord_port,
        })
    }

    /// Get peers sorted by rank
    pub fn sort_peers_by_rank(mut peers: Vec<PeerPod>) -> Vec<PeerPod> {
        peers.sort_by_key(|p| p.rank);
        peers
    }
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("Failed to create Kubernetes client: {0}")]
    KubeClient(#[source] kube::Error),
    #[error("Kubernetes API error: {0}")]
    KubeApi(#[source] kube::Error),
    #[error("Timed out waiting for all pods to be ready")]
    Timeout,
    #[error("Pod watch stream ended unexpectedly")]
    WatchEnded,
}
