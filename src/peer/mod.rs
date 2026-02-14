use std::{collections::HashMap, ffi, net};

#[cfg(feature = "test-bins")]
mod dir;
pub mod k8s;

#[cfg(feature = "test-bins")]
pub use dir::DirectoryPeers;
pub use k8s::KubernetesPeers;

pub trait PeerDiscovery {
    async fn peer(&self, node_rank: u32) -> net::SocketAddr;
    async fn peers(&self) -> HashMap<u32, net::SocketAddr>;
    fn local_ranks(&self, nproc: u16) -> impl Iterator<Item = u32>;
    fn hostnames(&self) -> impl Iterator<Item = ffi::CString>;
}
