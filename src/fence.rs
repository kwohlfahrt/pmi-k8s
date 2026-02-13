use std::net::SocketAddr;
use std::time::Duration;
use std::{ffi, io, slice};

use futures::future::{join, join_all};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net, time};

use crate::peer::PeerDiscovery;
use crate::pmix::{char_to_u8, globals, sys, u8_to_char};

pub struct NetFence<'a, D: PeerDiscovery> {
    listener: net::TcpListener,
    discovery: &'a D,
}

impl<'a, D: PeerDiscovery> NetFence<'a, D> {
    pub async fn new(addr: SocketAddr, discovery: &'a D) -> Self {
        let listener: net::TcpListener = net::TcpListener::bind(addr).await.unwrap();
        Self {
            listener,
            discovery,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        // We know we have bound a local socket, so this can be unwrapped.
        #[allow(clippy::unwrap_used)]
        self.listener.local_addr().unwrap()
    }

    fn read_data(data: globals::CData) -> Vec<u8> {
        // TODO: make a wrapper type for globals::CData to avoid the copy.
        let (ptr, _) = data;
        let data = if !data.0.is_null() {
            let slice = unsafe { slice::from_raw_parts(data.0, data.1) };
            char_to_u8(slice).to_vec()
        } else {
            Vec::new()
        };
        unsafe { libc::free(ptr as *mut ffi::c_void) };

        data
    }

    async fn recv(&self, n: usize) -> Vec<u8> {
        stream::iter(0..n)
            .then(|_| self.listener.accept())
            .try_fold(Vec::new(), async |mut acc, (mut c, _)| {
                c.read_to_end(&mut acc).await?;
                Ok(acc)
            })
            .await
            .unwrap()
    }

    async fn send(addr: &SocketAddr, data: &[u8]) {
        let mut s = loop {
            match net::TcpStream::connect(addr).await {
                Ok(s) => break s,
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    // TODO: Proper backoff
                    time::sleep(Duration::from_millis(250)).await
                }
                Err(e) => panic!("unexpected send error: {e}"),
            }
        };
        s.write_all(data).await.unwrap();
    }

    async fn submit_data(&self, procs: &[sys::pmix_proc_t], data: &[u8]) -> Vec<u8> {
        // TODO: Handle other fence scopes
        assert_eq!(procs.len(), 1);
        assert_eq!(procs[0].rank, sys::PMIX_RANK_WILDCARD);

        // TODO: exclude ourselves from send + recv
        let peers = self.discovery.peers().await;
        let sends = peers.values().map(|addr| Self::send(&addr, &data));
        let acc = self.recv(peers.len());

        join(acc, join_all(sends)).await.0
    }

    pub async fn submit(
        &self,
        procs: &[sys::pmix_proc_t],
        data: globals::CData,
        callback: globals::ModexCallback,
    ) {
        let (Some(cbfunc), cbdata) = callback else {
            return;
        };
        let data = Self::read_data(data);
        let acc = Box::new(self.submit_data(procs, &data).await);
        let data = u8_to_char(&acc);
        unsafe {
            cbfunc(
                sys::PMIX_SUCCESS as sys::pmix_status_t,
                data.as_ptr(),
                data.len(),
                cbdata,
                Some(globals::release_vec_u8),
                Box::into_raw(acc) as *mut ffi::c_void,
            )
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, net::Ipv4Addr};

    use super::*;
    use crate::peer::DirectoryPeers;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_fence() {
        let n = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), n);
        let fences = join_all((0..n).map(|_| {
            let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
            NetFence::new(addr, &discovery)
        }))
        .await;
        for f in fences.iter() {
            discovery.register(&f.addr());
        }
        let procs = [sys::pmix_proc_t {
            nspace: [0; _],
            rank: sys::PMIX_RANK_WILDCARD,
        }];
        let results = join_all(fences.iter().enumerate().map(async |(i, f)| {
            let data = [i as u8];
            f.submit_data(&procs, &data).await
        }));

        let expected = (0..n as u8).collect::<HashSet<_>>();
        for result in results.await {
            let result = result.into_iter().collect::<HashSet<_>>();
            assert_eq!(result, expected)
        }
    }
}
