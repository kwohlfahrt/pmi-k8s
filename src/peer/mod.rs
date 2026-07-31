use std::{error::Error, net};

#[cfg(feature = "test-bins")]
mod dir;
pub mod k8s;

#[cfg(feature = "test-bins")]
pub use dir::DirectoryPeers;
pub use k8s::KubernetesPeers;

use crate::pmix::sys;

#[derive(Clone, Copy, Debug)]
pub enum Endpoint {
    Fence,
    Modex,
}

pub trait PeerDiscovery {
    type Error: Error;

    async fn peer(
        &self,
        proc: &sys::pmix_proc_t,
        endpoint: Endpoint,
    ) -> Result<net::SocketAddr, Self::Error>;
    async fn peers(
        &self,
        procs: &[sys::pmix_proc_t],
        endpoint: Endpoint,
    ) -> Result<Vec<net::SocketAddr>, Self::Error>;

    fn local_ranks(&self) -> impl Iterator<Item = u32>;
    fn hostnames(&self) -> impl Iterator<Item = String>;
    fn node_rank(&self) -> u32;
}
