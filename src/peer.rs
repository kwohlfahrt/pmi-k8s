// FIXME: This is only used in testing, make the dependency dev-only
use notify::{self, Watcher};
use std::{collections::HashMap, fs, net, path::Path, sync::mpsc};

pub struct DirPeerDiscovery<'a> {
    dir: &'a Path,
    nnodes: u32,
}

impl<'a> DirPeerDiscovery<'a> {
    pub fn new(dir: &'a Path, nnodes: u32) -> Self {
        DirPeerDiscovery { dir, nnodes }
    }

    fn read_peer(path: &Path) -> net::SocketAddr {
        fs::read_to_string(path).unwrap().trim().parse().unwrap()
    }

    fn wait_for_peer(&self, path: &Path) -> net::SocketAddr {
        let (tx, rx) = mpsc::channel();
        let mut watcher = notify::recommended_watcher(tx).unwrap();
        watcher
            .watch(self.dir, notify::RecursiveMode::NonRecursive)
            .unwrap();

        if path.exists() {
            return Self::read_peer(path);
        }

        loop {
            let event = rx.recv().unwrap().unwrap();
            if event.kind == notify::EventKind::Create(notify::event::CreateKind::File) {
                if event.paths.iter().any(|p| p == path) {
                    break Self::read_peer(path);
                }
            }
        }
    }

    pub fn peer(&self, node_rank: u32) -> net::SocketAddr {
        let path = self.dir.join(format!("{}", node_rank));
        if path.exists() {
            Self::read_peer(&path)
        } else {
            self.wait_for_peer(&path)
        }
    }

    pub fn peers(&self) -> HashMap<u32, net::SocketAddr> {
        (0..self.nnodes)
            .map(|node_rank| (node_rank, self.peer(node_rank)))
            .collect()
    }

    pub fn register(&self, addr: &net::SocketAddr, node_rank: u32) {
        let path = self.dir.join(node_rank.to_string());
        fs::write(path, addr.to_string()).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::thread;
    use tempdir::TempDir;

    #[test]
    fn test_dir_discovery() {
        let dir = TempDir::new("discovery-test").unwrap();
        let n = 2;
        let discovery = DirPeerDiscovery::new(dir.path(), n);
        let expected = (0..n as u16)
            .map(|i| {
                (
                    i as u32,
                    net::SocketAddr::new(net::Ipv4Addr::new(127, 0, 0, 1).into(), 5000 + i),
                )
            })
            .collect::<HashMap<u32, _>>();

        let peers = thread::scope(|scope| {
            let t = scope.spawn(|| discovery.peers());
            for (i, addr) in &expected {
                discovery.register(addr, *i);
            }
            t.join().unwrap()
        });
        assert_eq!(peers, expected);
    }
}
