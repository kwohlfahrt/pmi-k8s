use futures::{TryStreamExt, stream::FuturesUnordered};
use notify::{self, Watcher};
use std::{
    cell::RefCell,
    collections::HashMap,
    ffi, fs,
    io::{self, Write},
    net,
    path::Path,
};
use tokio::sync::mpsc;

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
    nnodes: u32,
    node_rank: RefCell<Option<u32>>,
}

impl<'a> DirectoryPeers<'a> {
    pub fn new(dir: &'a Path, nnodes: u32) -> Self {
        DirectoryPeers {
            dir,
            nnodes,
            node_rank: RefCell::new(None),
        }
    }

    fn read_peer(path: &Path) -> Result<net::SocketAddr, Error> {
        Ok(fs::read_to_string(path)?.parse()?)
    }

    async fn wait_for_peer(&self, path: &Path) -> Result<net::SocketAddr, Error> {
        if path.exists() {
            // Fast path for if path already exists
            return Self::read_peer(path);
        }

        let (tx, mut rx) = mpsc::channel(1);
        #[allow(clippy::unwrap_used, reason = "watcher is dropped before the receiver")]
        let mut watcher = notify::recommended_watcher(move |res| tx.blocking_send(res).unwrap())?;
        watcher.watch(self.dir, notify::RecursiveMode::NonRecursive)?;

        if path.exists() {
            // Handle race condition between fast-path and setting up watch
            return Self::read_peer(path);
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
                break Self::read_peer(path);
            }
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

    async fn peer(&self, node_rank: u32) -> Result<net::SocketAddr, Error> {
        let path = self.dir.join(format!("{}", node_rank));
        if path.exists() {
            Ok(Self::read_peer(&path)?)
        } else {
            Ok(self.wait_for_peer(&path).await?)
        }
    }

    async fn peers(&self) -> Result<HashMap<u32, net::SocketAddr>, Error> {
        (0..self.nnodes)
            .map(async |node_rank| self.peer(node_rank).await.map(|peer| (node_rank, peer)))
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await
    }

    fn local_ranks(&self, nprocs: u16) -> impl Iterator<Item = u32> {
        let node_rank = self.node_rank.borrow().expect("Node is not registered");
        (node_rank * nprocs as u32)..((node_rank + 1) * nprocs as u32)
    }

    fn hostnames(&self) -> impl Iterator<Item = std::ffi::CString> {
        // These hostnames don't actually resolve, but that doesn't seem to matter.
        (0..self.nnodes).map(|rank| {
            #[allow(clippy::unwrap_used, reason = "Literal string without NULLs")]
            ffi::CString::new(format!("mpi-{}", rank)).unwrap()
        })
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
        let discovery = DirectoryPeers::new(dir.path(), n);
        let expected = (0..n as u16)
            .map(|i| net::SocketAddr::new(net::Ipv4Addr::new(127, 0, 0, 1).into(), 5000 + i))
            .collect::<HashSet<_>>();

        for addr in &expected {
            discovery.register(addr).unwrap();
        }

        let peers = discovery
            .peers()
            .await
            .unwrap()
            .into_values()
            .collect::<HashSet<_>>();
        assert_eq!(peers, expected);
    }
}
