use std::{collections::HashMap, error::Error, ffi, net};

#[cfg(feature = "test-bins")]
mod dir;
pub mod k8s;

#[cfg(feature = "test-bins")]
pub use dir::DirectoryPeers;
pub use k8s::KubernetesPeers;

pub trait PeerDiscovery {
    type Error: Error;

    async fn peer(&self, node_rank: u32) -> Result<net::SocketAddr, Self::Error>;
    async fn peers(&self) -> Result<HashMap<u32, net::SocketAddr>, Self::Error>;

    fn local_ranks(&self, nproc: u16) -> impl Iterator<Item = u32>;
    fn hostnames(&self) -> impl Iterator<Item = ffi::CString>;
}
