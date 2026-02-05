// FIXME: This is only used in testing, make the dependency dev-only
use notify::{self, Watcher};
use std::{collections::HashSet, fs, net, path::Path, sync::mpsc};

pub struct DirPeerDiscovery<'a> {
    dir: &'a Path,
    n: u32,
}

impl<'a> DirPeerDiscovery<'a> {
    pub fn new(dir: &'a Path, n: u32) -> Self {
        DirPeerDiscovery { dir, n }
    }

    fn read_peer(path: &Path) -> net::SocketAddr {
        fs::read_to_string(path).unwrap().trim().parse().unwrap()
    }

    pub fn peers(&self) -> HashSet<net::SocketAddr> {
        let (tx, rx) = mpsc::channel();
        let mut watcher = notify::recommended_watcher(tx).unwrap();
        watcher
            .watch(self.dir, notify::RecursiveMode::NonRecursive)
            .unwrap();

        let mut addrs = fs::read_dir(&self.dir)
            .unwrap()
            .map(|e| Self::read_peer(&e.unwrap().path()))
            .collect::<HashSet<_>>();
        while addrs.len() < self.n as usize {
            let event = rx.recv().unwrap().unwrap();
            if event.kind == notify::EventKind::Create(notify::event::CreateKind::File) {
                event.paths.into_iter().for_each(|p| {
                    addrs.insert(Self::read_peer(&p));
                });
            }
        }
        addrs
    }

    pub fn register(&self, addr: &net::SocketAddr) {
        let path = self.dir.join(format!("{}", addr.port()));
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
            .map(|i| net::SocketAddr::new(net::Ipv4Addr::new(127, 0, 0, 1).into(), 5000 + i))
            .collect::<HashSet<_>>();
        let peers = thread::scope(|scope| {
            let t = scope.spawn(|| discovery.peers());
            for addr in &expected {
                discovery.register(addr);
            }
            t.join().unwrap()
        });
        assert_eq!(peers, expected);
    }
}
