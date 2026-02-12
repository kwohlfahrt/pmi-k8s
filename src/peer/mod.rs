use std::{collections::HashMap, net};

#[cfg(feature = "test-bins")]
mod dir;
pub mod k8s;

#[cfg(feature = "test-bins")]
pub use dir::DirectoryPeers;
pub use k8s::KubernetesPeers;

#[allow(async_fn_in_trait)]
pub trait PeerDiscovery {
    async fn peer(&self, node_rank: u32) -> net::SocketAddr;
    async fn peers(&self) -> HashMap<u32, net::SocketAddr>;
}
