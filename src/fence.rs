use std::net::SocketAddr;
use std::time::Duration;
use std::{ffi, io, slice};

use futures::future::join;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net, time};

use super::ModexError;
use crate::peer::PeerDiscovery;
use crate::pmix::{char_to_u8, globals, sys};

pub struct NetFence<'a, D: PeerDiscovery> {
    listener: net::TcpListener,
    discovery: &'a D,
}

impl<'a, D: PeerDiscovery> NetFence<'a, D> {
    pub async fn new(addr: SocketAddr, discovery: &'a D) -> Result<Self, ModexError<D::Error>> {
        let listener: net::TcpListener = net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            discovery,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        #[allow(clippy::unwrap_used, reason = "We know we have a socket bound")]
        self.listener.local_addr().unwrap()
    }

    fn load_data(data: globals::CData) -> Vec<u8> {
        // TODO: make a wrapper type for globals::CData to avoid the copy.
        let (ptr, _) = data;
        let data = if !data.0.is_null() {
            // SAFETY: Data is a pointer to [ffi::c_char; sz], and we have
            // checked for `null` ourselves.
            let slice = unsafe { slice::from_raw_parts(data.0, data.1) };
            char_to_u8(slice).to_vec()
        } else {
            Vec::new()
        };

        // SAFETY: PMIx standard says the user is responsible for `free`ing the
        // data, assuming this means `libc::free`.
        unsafe { libc::free(ptr as *mut ffi::c_void) };

        data
    }

    async fn recv(&self, n: usize) -> io::Result<Vec<u8>> {
        stream::iter(0..n)
            .then(|_| self.listener.accept())
            .try_fold(Vec::new(), async |mut acc, (mut c, _)| {
                c.read_to_end(&mut acc).await?;
                Ok(acc)
            })
            .await
    }

    async fn send(addr: &SocketAddr, data: &[u8]) -> io::Result<()> {
        let mut s = loop {
            match net::TcpStream::connect(addr).await {
                Ok(s) => break s,
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    // TODO: Proper backoff
                    time::sleep(Duration::from_millis(250)).await
                }
                Err(e) => return Err(e),
            }
        };
        s.write_all(data).await?;
        Ok(())
    }

    async fn submit_data(
        &self,
        procs: &[sys::pmix_proc_t],
        data: &[u8],
    ) -> Result<Vec<u8>, ModexError<D::Error>> {
        // TODO: Handle other fence scopes
        assert_eq!(procs.len(), 1);
        assert_eq!(procs[0].rank, sys::PMIX_RANK_WILDCARD);

        // TODO: exclude ourselves from send + recv
        let peers = self.discovery.peers().await.map_err(ModexError::Peer)?;
        let sends = peers
            .values()
            .map(|addr| Self::send(addr, data))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>();
        let acc = self.recv(peers.len());

        let (data, sends) = join(acc, sends).await;
        sends.and(data).map_err(|e| e.into())
    }

    pub async fn submit(
        &self,
        procs: &[sys::pmix_proc_t],
        data: globals::CData,
        callback: globals::ModexCallback,
    ) -> Result<(), ModexError<D::Error>> {
        let data = Self::load_data(data);
        let data = self.submit_data(procs, &data).await?;
        callback.call(sys::PMIX_SUCCESS as sys::pmix_status_t, data);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use std::{collections::HashSet, net::Ipv4Addr};

    use super::*;
    use crate::peer::DirectoryPeers;
    use futures::future::join_all;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_fence() {
        let n = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), n);
        let fences = join_all((0..n).map(async |_| {
            let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
            NetFence::new(addr, &discovery).await.unwrap()
        }))
        .await;
        for f in fences.iter() {
            discovery.register(&f.addr()).unwrap();
        }
        let procs = [sys::pmix_proc_t {
            nspace: [0; _],
            rank: sys::PMIX_RANK_WILDCARD,
        }];
        let results = join_all(fences.iter().enumerate().map(async |(i, f)| {
            let data = [i as u8];
            f.submit_data(&procs, &data).await.unwrap()
        }));

        let expected = (0..n as u8).collect::<HashSet<_>>();
        for result in results.await {
            let result = result.into_iter().collect::<HashSet<_>>();
            assert_eq!(result, expected)
        }
    }
}
