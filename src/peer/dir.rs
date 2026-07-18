use futures::{TryStreamExt, stream::FuturesUnordered};
use notify::{self, Watcher};
use std::{
    cell::RefCell,
    collections::HashSet,
    ffi, fs,
    io::{self, Write},
    net,
    path::Path,
};
use tokio::sync::mpsc;

use crate::{peer::Endpoint, pmix::sys};

use super::PeerDiscovery;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to read or write peer information")]
    Io(#[from] io::Error),
    #[error("unable to watch for new peers")]
    Notify(#[from] notify::Error),
    #[error("unable to parse data")]
    InvalidAddr(#[from] net::AddrParseError),
}

pub struct DirectoryPeers<'a> {
    dir: &'a Path,
    nproc: u16,
    nnodes: u32,
    node_rank: RefCell<Option<u32>>,
}

impl<'a> DirectoryPeers<'a> {
    pub fn new(dir: &'a Path, nproc: u16, nnodes: u32) -> Self {
        DirectoryPeers {
            dir,
            nproc,
            nnodes,
            node_rank: RefCell::new(None),
        }
    }

    fn read_node(path: &Path) -> Result<net::SocketAddr, Error> {
        Ok(fs::read_to_string(path)?.parse()?)
    }

    async fn wait_for_node(&self, path: &Path) -> Result<net::SocketAddr, Error> {
        if path.exists() {
            // Fast path for if path already exists
            return Self::read_node(path);
        }

        let (tx, mut rx) = mpsc::channel(1);
        #[allow(clippy::unwrap_used, reason = "watcher is dropped before the receiver")]
        let mut watcher = notify::recommended_watcher(move |res| tx.blocking_send(res).unwrap())?;
        watcher.watch(self.dir, notify::RecursiveMode::NonRecursive)?;

        if path.exists() {
            // Handle race condition between fast-path and setting up watch
            return Self::read_node(path);
        }

        loop {
            #[allow(
                clippy::unwrap_used,
                reason = "sender is not dropped until the last iteration"
            )]
            let event = rx.recv().await.unwrap()?;
            if event.kind == notify::EventKind::Create(notify::event::CreateKind::File)
                && event.paths.iter().any(|p| p == path)
            {
                drop(watcher);
                break Self::read_node(path);
            }
        }
    }

    // This is for unit testing only, where we never test both modex + fence in
    // the same run. So the endpoint doesn't matter.
    async fn node(&self, node_rank: u32, _endpoint: Endpoint) -> Result<net::SocketAddr, Error> {
        let path = self.dir.join(format!("{}", node_rank));
        if path.exists() {
            Ok(Self::read_node(&path)?)
        } else {
            Ok(self.wait_for_node(&path).await?)
        }
    }

    pub fn register(&self, addr: &net::SocketAddr) -> io::Result<()> {
        let (node_rank, mut f) = (0..self.nnodes)
            .map(|node_rank| {
                (
                    node_rank,
                    fs::File::create_new(self.dir.join(node_rank.to_string())),
                )
            })
            .filter_map(|(node_rank, f)| match f {
                Ok(f) => Some(Ok((node_rank, f))),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => None,
                Err(e) => Some(Err(e)),
            })
            .next()
            .expect("All nodes already registered")
            .expect("Error registering node");

        f.write_all(addr.to_string().as_bytes())?;
        *self.node_rank.borrow_mut() = Some(node_rank);
        Ok(())
    }
}

impl<'a> PeerDiscovery for DirectoryPeers<'a> {
    type Error = Error;

    async fn peer(
        &self,
        proc: &sys::pmix_proc_t,
        endpoint: Endpoint,
    ) -> Result<net::SocketAddr, Error> {
        assert!(proc.rank <= sys::PMIX_RANK_VALID);

        let node_rank = proc.rank / (self.nproc as u32);
        self.node(node_rank, endpoint).await
    }

    async fn peers(
        &self,
        procs: &[sys::pmix_proc_t],
        endpoint: Endpoint,
    ) -> Result<Vec<net::SocketAddr>, Error> {
        if let [
            sys::pmix_proc_t {
                rank: sys::PMIX_RANK_WILDCARD,
                // TODO: Handle other namespaces
                nspace: _,
            },
        ] = procs
        {
            (0..self.nnodes)
                .map(async |node_rank| self.node(node_rank, endpoint).await)
                .collect::<FuturesUnordered<_>>()
                .try_collect()
                .await
        } else {
            let nodes = procs
                .iter()
                .map(|sys::pmix_proc_t { rank, nspace: _ }| rank / (self.nproc as u32))
                .collect::<HashSet<_>>();

            nodes
                .into_iter()
                .map(async |node_rank| self.node(node_rank, endpoint).await)
                .collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>()
                .await
        }
    }

    fn local_ranks(&self) -> impl Iterator<Item = u32> {
        let node_rank = self.node_rank.borrow().expect("Node is not registered");
        (node_rank * self.nproc as u32)..((node_rank + 1) * self.nproc as u32)
    }

    fn hostnames(&self) -> impl Iterator<Item = std::ffi::CString> {
        // These hostnames don't actually resolve, but that doesn't seem to matter.
        (0..self.nnodes).map(|rank| {
            #[allow(clippy::unwrap_used, reason = "Literal string without NULLs")]
            ffi::CString::new(format!("mpi-{}", rank)).unwrap()
        })
    }

    fn node_rank(&self) -> u32 {
        self.node_rank.borrow().expect("Node is not registered")
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use std::collections::HashSet;

    use super::*;

    use tempdir::TempDir;

    #[tokio::test]
    async fn test_dir_discovery() {
        let dir = TempDir::new("discovery-test").unwrap();
        let n = 2;
        let nproc = 4;
        let discovery = DirectoryPeers::new(dir.path(), nproc, n);
        let expected = (0..n as u16)
            .map(|i| net::SocketAddr::new(net::Ipv4Addr::new(127, 0, 0, 1).into(), 5000 + i))
            .collect::<HashSet<_>>();

        for addr in &expected {
            discovery.register(addr).unwrap();
        }

        let wildcard = [sys::pmix_proc_t {
            rank: sys::PMIX_RANK_WILDCARD,
            nspace: [0; _],
        }];

        let peers = discovery
            .peers(&wildcard, Endpoint::Fence)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(peers, expected);

        let enumerated = (0..(n * nproc as u32))
            .map(|rank| sys::pmix_proc_t {
                rank,
                nspace: [0; _],
            })
            .collect::<Vec<_>>();
        let peers = discovery
            .peers(&enumerated, Endpoint::Fence)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(peers, expected);
    }
}
