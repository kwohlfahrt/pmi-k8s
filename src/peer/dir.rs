// FIXME: This is only used in testing, make the dependency dev-only
use futures::future::join_all;
use notify::{self, Watcher};
use std::{collections::HashMap, fs, net, path::Path};
use tokio::sync::mpsc;

pub struct PeerDiscovery<'a> {
    dir: &'a Path,
    nnodes: u32,
}

impl<'a> PeerDiscovery<'a> {
    pub fn new(dir: &'a Path, nnodes: u32) -> Self {
        PeerDiscovery { dir, nnodes }
    }

    fn read_peer(path: &Path) -> net::SocketAddr {
        fs::read_to_string(path).unwrap().trim().parse().unwrap()
    }

    async fn wait_for_peer(&self, path: &Path) -> net::SocketAddr {
        if path.exists() {
            // Fast path for if path already exists
            return Self::read_peer(path);
        }

        let (tx, mut rx) = mpsc::channel(1);
        let mut watcher =
            notify::recommended_watcher(move |res| tx.blocking_send(res).unwrap()).unwrap();
        watcher
            .watch(self.dir, notify::RecursiveMode::NonRecursive)
            .unwrap();

        if path.exists() {
            // Handle race condition between fast-path and setting up watch
            return Self::read_peer(path);
        }

        loop {
            let event = rx.recv().await.unwrap().unwrap();
            if event.kind == notify::EventKind::Create(notify::event::CreateKind::File) {
                if event.paths.iter().any(|p| p == path) {
                    break Self::read_peer(path);
                }
            }
        }
    }

    pub async fn peer(&self, node_rank: u32) -> net::SocketAddr {
        let path = self.dir.join(format!("{}", node_rank));
        if path.exists() {
            Self::read_peer(&path)
        } else {
            self.wait_for_peer(&path).await
        }
    }

    pub async fn peers(&self) -> HashMap<u32, net::SocketAddr> {
        let peers = (0..self.nnodes).map(async |node_rank| (node_rank, self.peer(node_rank).await));
        join_all(peers).await.into_iter().collect()
    }

    pub fn register(&self, addr: &net::SocketAddr, node_rank: u32) {
        let path = self.dir.join(node_rank.to_string());
        fs::write(path, addr.to_string()).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempdir::TempDir;

    #[tokio::test]
    async fn test_dir_discovery() {
        let dir = TempDir::new("discovery-test").unwrap();
        let n = 2;
        let discovery = PeerDiscovery::new(dir.path(), n);
        let expected = (0..n as u16)
            .map(|i| {
                (
                    i as u32,
                    net::SocketAddr::new(net::Ipv4Addr::new(127, 0, 0, 1).into(), 5000 + i),
                )
            })
            .collect::<HashMap<u32, _>>();

        for (i, addr) in &expected {
            discovery.register(addr, *i);
        }

        let peers = discovery.peers();
        assert_eq!(peers.await, expected);
    }
}
