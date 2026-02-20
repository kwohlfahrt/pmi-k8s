use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures::future::join;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net, time};

use super::ModexError;
use crate::peer::PeerDiscovery;
use crate::pmix::{globals, sys};

pub struct NetFence<'a, D: PeerDiscovery> {
    listener: net::TcpListener,
    discovery: &'a D,
    nprocs: u16,
}

impl<'a, D: PeerDiscovery> NetFence<'a, D> {
    pub async fn new(
        addr: SocketAddr,
        nprocs: u16,
        discovery: &'a D,
    ) -> Result<Self, ModexError<D::Error>> {
        let listener: net::TcpListener = net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            discovery,
            nprocs,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        #[allow(clippy::unwrap_used, reason = "We know we have a socket bound")]
        self.listener.local_addr().unwrap()
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
        // TODO: Handle other namespaces
        let peers = if procs.len() == 1 && procs[0].rank == sys::PMIX_RANK_WILDCARD {
            self.discovery.peers().await
        } else {
            let node_ranks = procs
                .iter()
                .map(|proc| proc.rank / self.nprocs as u32)
                .collect::<HashSet<_>>();

            node_ranks
                .into_iter()
                .map(async |node_rank| {
                    self.discovery
                        .peer(node_rank)
                        .await
                        .map(|addr| (node_rank, addr))
                })
                .collect::<FuturesUnordered<_>>()
                .try_collect::<HashMap<_, _>>()
                .await
        }
        .map_err(ModexError::Peer)?;

        // TODO: exclude ourselves from send + recv
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

    async fn create_fences<'a>(
        nnodes: u32,
        discovery: &'a DirectoryPeers<'a>,
    ) -> Vec<NetFence<'a, DirectoryPeers<'a>>> {
        let fences = join_all((0..nnodes).map(async |_| {
            let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
            NetFence::new(addr, 1, discovery).await.unwrap()
        }))
        .await;
        for f in fences.iter() {
            discovery.register(&f.addr()).unwrap();
        }
        fences
    }

    #[tokio::test]
    async fn test_global_fence() {
        let nnodes = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), nnodes);
        let fences = create_fences(nnodes, &discovery).await;

        let procs = [sys::pmix_proc_t {
            nspace: [0; _],
            rank: sys::PMIX_RANK_WILDCARD,
        }];
        let results = join_all(fences.iter().enumerate().map(async |(i, f)| {
            let data = [i as u8];
            f.submit_data(&procs, &data).await.unwrap()
        }));

        let expected = (0..nnodes as u8).collect::<HashSet<_>>();
        for result in results.await {
            let result = result.into_iter().collect::<HashSet<_>>();
            assert_eq!(result, expected)
        }
    }

    #[tokio::test]
    async fn test_partial_fence() {
        let nnodes = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), nnodes);
        let fences = create_fences(nnodes, &discovery).await;

        let n_fence = 3;
        let procs = (0..n_fence)
            .map(|rank| sys::pmix_proc_t {
                nspace: [0; _],
                rank,
            })
            .collect::<Vec<_>>();

        let results = join_all(procs.iter().map(async |proc| {
            let data = [proc.rank as u8];
            let fence = &fences[proc.rank as usize];
            fence.submit_data(&procs, &data).await.unwrap()
        }));

        let expected = (0..n_fence as u8).collect::<HashSet<_>>();
        for result in results.await {
            let result = result.into_iter().collect::<HashSet<_>>();
            assert_eq!(result, expected)
        }
    }

    #[tokio::test]
    async fn test_overlapping_fence() {
        let nnodes = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), nnodes);
        let fences = create_fences(nnodes, &discovery).await;

        let fence_rankss = [(0..3), (1..4)];
        let procss = fence_rankss.iter().map(|ranks| {
            ranks
                .clone()
                .map(|rank| sys::pmix_proc_t {
                    nspace: [0; _],
                    rank,
                })
                .collect::<Vec<_>>()
        });

        let resultss = procss.into_iter().map(async |procs| {
            join_all(procs.iter().map(async |proc| {
                let data = [proc.rank as u8];
                let fence = &fences[proc.rank as usize];
                fence.submit_data(&procs, &data).await.unwrap()
            }))
            .await
        });
        let resultss = join_all(resultss);

        let expecteds = fence_rankss.map(|ranks| ranks.map(|r| r as u8).collect::<HashSet<_>>());
        for (results, expected) in resultss.await.into_iter().zip(expecteds) {
            for result in results {
                let result = result.into_iter().collect::<HashSet<_>>();
                assert_eq!(result, expected)
            }
        }
    }
}
