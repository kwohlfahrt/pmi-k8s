use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::{io, mem};

use futures::stream::FuturesUnordered;
use futures::{FutureExt, select, stream};
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net;
use tokio::sync::mpsc;
use tracing::warn;

use super::ModexError;
use crate::net::connect_peer;
use crate::peer::{Endpoint, PeerDiscovery};
use crate::pmix::{char_to_u8, globals, sys, u8_to_char};

type Sequence = u32;
type Participants = BTreeSet<sys::pmix_proc_t>;

#[derive(PartialEq, Eq, Hash, Clone)]
struct FenceId(Participants, Sequence);

#[derive(Default)]
struct FenceAcc {
    complete: usize,
    data: Vec<u8>,
    cb: Option<globals::ModexCallback>,
    expected: Option<usize>,
}

impl FenceAcc {
    fn update(&mut self, data: FenceData) {
        match data {
            FenceData::Local(npeers) => {
                let _ = self.expected.insert(npeers);
            }
            FenceData::Remote(data) => {
                self.data.extend(data);
                self.complete += 1
            }
        };
    }

    fn complete(&mut self) -> Option<(globals::ModexCallback, Vec<u8>)> {
        if self.expected != Some(self.complete) {
            None
        } else if let Some(cb) = self.cb.take() {
            Some((cb, mem::take(&mut self.data)))
        } else {
            None
        }
    }
}

enum FenceData {
    Local(usize),
    Remote(Vec<u8>),
}

pub struct NetFence<'a, D> {
    listener: net::TcpListener,
    sequences: HashMap<Participants, Sequence>,
    in_flight: HashMap<FenceId, FenceAcc>,
    discovery: &'a D,
}

impl<'a, D: PeerDiscovery> NetFence<'a, D> {
    pub async fn new(addr: SocketAddr, discovery: &'a D) -> Result<Self, ModexError<D::Error>> {
        let listener: net::TcpListener = net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            discovery,
            sequences: Default::default(),
            in_flight: Default::default(),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        #[allow(clippy::unwrap_used, reason = "We know we have a socket bound")]
        self.listener.local_addr().unwrap()
    }

    fn serialize_proc(proc: &sys::pmix_proc_t) -> Vec<u8> {
        let mut s = Vec::with_capacity(mem::size_of::<sys::pmix_proc_t>());
        s.extend_from_slice(char_to_u8(&proc.nspace));
        s.extend_from_slice(&proc.rank.to_be_bytes());
        s
    }

    fn serialize_header(FenceId(participants, seq): &FenceId) -> Vec<u8> {
        let nproc = participants.len() as sys::pmix_rank_t;
        let mut buf = Vec::with_capacity(
            mem::size_of_val(&nproc)
                + ((nproc as usize) * mem::size_of::<sys::pmix_proc_t>())
                + mem::size_of::<Sequence>(),
        );
        buf.extend_from_slice(&nproc.to_be_bytes());
        for proc in participants.iter() {
            buf.extend_from_slice(&Self::serialize_proc(proc));
        }
        buf.extend_from_slice(&seq.to_be_bytes());
        buf
    }

    fn parse_proc(buf: [u8; mem::size_of::<sys::pmix_proc_t>()]) -> sys::pmix_proc_t {
        let (nspace, rank) = buf.split_at(mem::size_of::<sys::pmix_nspace_t>());
        #[allow(clippy::unwrap_used, reason = "Sizes are statically known")]
        let rank = u32::from_be_bytes(rank.try_into().unwrap());
        #[allow(clippy::unwrap_used, reason = "Sizes are statically known")]
        let nspace = u8_to_char(nspace).try_into().unwrap();
        sys::pmix_proc_t { rank, nspace }
    }

    async fn parse_header(c: &mut net::TcpStream) -> Result<FenceId, io::Error> {
        let mut buf = [0; mem::size_of::<sys::pmix_rank_t>()];
        c.read_exact(buf.as_mut_slice()).await?;
        let nproc = sys::pmix_rank_t::from_be_bytes(buf);

        let mut procs = Participants::new();
        for _ in 0..nproc {
            let mut buf = [0; mem::size_of::<sys::pmix_proc_t>()];
            c.read_exact(buf.as_mut_slice()).await?;
            procs.insert(Self::parse_proc(buf));
        }

        c.read_exact(buf.as_mut_slice()).await?;
        let seq = Sequence::from_be_bytes(buf);
        Ok(FenceId(procs, seq))
    }

    async fn send(
        peers: Vec<SocketAddr>,
        header: Vec<u8>,
        data: globals::CData,
    ) -> Result<(), io::Error> {
        stream::iter(peers)
            .map(Ok)
            .try_for_each(async |peer| {
                let mut s = connect_peer(&peer).await?;
                s.write_all(&header).await?;
                s.write_all(&data).await?;
                Ok(())
            })
            .await
    }

    fn fence_id(&mut self, procs: Vec<sys::pmix_proc_t>) -> FenceId {
        let participants = procs.into_iter().collect::<BTreeSet<_>>();
        let curr = self.sequences.entry(participants.clone()).or_default();
        let seq = *curr;
        *curr += 1;
        FenceId(participants, seq)
    }

    fn accept_event(
        &mut self,
        e: globals::FenceEvent,
    ) -> impl Future<Output = Result<(FenceId, usize), ModexError<D::Error>>> + use<'a, D> {
        let globals::FenceEvent { procs, data, cb } = e;
        let id = self.fence_id(procs.clone());
        let acc = self.in_flight.entry(id.clone()).or_default();
        // Record the callback for future status reports. This must happen synchronously.
        let _ = acc.cb.insert(cb);

        let discovery = self.discovery;
        async move {
            let peers = discovery
                .peers(&procs, Endpoint::Fence)
                .await
                .map_err(ModexError::Peer)?;
            let npeers = peers.len();
            let header = Self::serialize_header(&id);
            Self::send(peers, header, data).await?;
            Ok((id, npeers))
        }
    }

    async fn accept_conn(mut c: net::TcpStream) -> Result<(FenceId, Vec<u8>), io::Error> {
        let id = Self::parse_header(&mut c).await?;
        let mut data = Vec::new();
        c.read_to_end(&mut data).await?;
        Ok((id, data))
    }

    fn complete_fence(&mut self, id: FenceId, data: FenceData) {
        let result = match self.in_flight.entry(id) {
            Entry::Occupied(mut e) => {
                let acc = e.get_mut();
                acc.update(data);
                if let Some(result) = acc.complete() {
                    e.remove();
                    Some(result)
                } else {
                    None
                }
            }
            Entry::Vacant(e) => {
                let mut acc = FenceAcc::default();
                acc.update(data);
                if let Some(result) = acc.complete() {
                    Some(result)
                } else {
                    e.insert(acc);
                    None
                }
            }
        };

        if let Some((cb, data)) = result {
            cb.call(sys::PMIX_SUCCESS as sys::pmix_status_t, data);
        }
    }

    pub async fn serve(
        mut self,
        mut events: mpsc::UnboundedReceiver<globals::FenceEvent>,
    ) -> Result<(), ModexError<D::Error>> {
        let mut local = FuturesUnordered::new();
        let mut remote = FuturesUnordered::new();

        let result = loop {
            select! {
                e = events.recv().fuse() => match e {
                    Some(e) => local.push(self.accept_event(e)),
                    None => break Ok(()),
                },
                c = self.listener.accept().fuse() => match c {
                    Ok((c, _)) => remote.push(Self::accept_conn(c)),
                    Err(err) => warn!(%err, "fence accept"),
                },
                l = local.select_next_some() => match l {
                    Ok((id, npeers)) => self.complete_fence(id, FenceData::Local(npeers)),
                    Err(err) => {
                        warn!(%err, "local fence");
                        break Err(err)
                    }
                },
                r = remote.select_next_some() => match r {
                    Ok((id, data)) => self.complete_fence(id, FenceData::Remote(data)),
                    Err(err) => {
                        warn!(%err, "remote fence");
                        break Err(err.into())
                    }
                },
            }
        };

        events.close(); // Stop accepting more events from the PMIx server
        while let Some(globals::FenceEvent { cb, .. }) = events.recv().await {
            cb.call(sys::PMIX_ERROR, Vec::new());
        }

        for cb in self
            .in_flight
            .drain()
            .flat_map(|(_, FenceAcc { cb, .. })| cb)
        {
            cb.call(sys::PMIX_ERROR, Vec::new());
        }

        result
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used, clippy::panic)]
    use std::{collections::HashSet, net::Ipv4Addr, pin::pin};

    use super::*;
    use crate::peer::DirectoryPeers;
    use futures::{
        TryFutureExt,
        future::{Either, join, join_all, select},
    };
    use tempdir::TempDir;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};

    type TestError<'a> = ModexError<<DirectoryPeers<'a> as PeerDiscovery>::Error>;

    async fn create_fence<'a>(
        discovery: &'a DirectoryPeers<'a>,
    ) -> (
        impl Future<Output = Result<(), TestError<'a>>>,
        mpsc::UnboundedSender<globals::FenceEvent>,
    ) {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let fence = NetFence::new(addr, discovery).await.unwrap();
        discovery.register(&fence.addr()).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        (fence.serve(rx), tx)
    }

    fn create_event(
        procs: Vec<sys::pmix_proc_t>,
        data: globals::CData,
    ) -> (
        globals::FenceEvent,
        oneshot::Receiver<(sys::pmix_status_t, Vec<u8>)>,
    ) {
        let (tx, rx) = oneshot::channel();
        let cb = globals::ModexCallback::test_callback(Box::new(move |status, data| {
            tx.send((status, Vec::from(data))).unwrap()
        }));
        (globals::FenceEvent { procs, data, cb }, rx)
    }

    #[tokio::test]
    async fn test_global_fence() {
        let nnodes = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), 1, nnodes);
        let (fences, txs) = join_all((0..nnodes).map(|_| create_fence(&discovery)))
            .await
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let results = txs.iter().enumerate().map(|(i, tx)| {
            let data = globals::CData::from_slice(&[i as u8]).unwrap();
            let procs = vec![sys::pmix_proc_t {
                nspace: [0; _],
                rank: sys::PMIX_RANK_WILDCARD,
            }];

            let (event, rx) = create_event(procs, data);
            tx.send(event).unwrap();
            rx
        });

        let Either::Left((results, _)) = select(join_all(results), join_all(fences)).await else {
            panic!("expected response");
        };

        let results = results
            .into_iter()
            .map(|r| r.map(|(_, v)| v.into_iter().collect::<HashSet<_>>()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let expected = (0..nnodes as u8).collect::<HashSet<_>>();
        for result in results {
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_partial_fence() {
        let nnodes = 4;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), 1, nnodes);
        let (fences, txs) = join_all((0..nnodes).map(|_| create_fence(&discovery)))
            .await
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let n_fence = 3;
        let procs = (0..n_fence)
            .map(|rank| sys::pmix_proc_t {
                nspace: [0; _],
                rank,
            })
            .collect::<Vec<_>>();

        let results = procs.iter().map(|proc| {
            let data = globals::CData::from_slice(&[proc.rank as u8]).unwrap();
            let (event, rx) = create_event(procs.clone(), data);
            let tx = &txs[proc.rank as usize];
            tx.send(event).unwrap();
            rx
        });

        let Either::Left((results, _)) = select(join_all(results), join_all(fences)).await else {
            panic!("expected response");
        };

        let results = results
            .into_iter()
            .map(|r| r.map(|(_, v)| v.into_iter().collect::<HashSet<_>>()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let expected = (0..n_fence as u8).collect::<HashSet<_>>();
        for result in results {
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_fence_cycle() {
        let nnodes = 3;
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), 1, nnodes);
        let (fences, txs) = join_all((0..nnodes).map(|_| create_fence(&discovery)))
            .await
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let procss = (0..nnodes)
            .map(|rank| {
                let nspace = [0; _];
                [
                    sys::pmix_proc_t { nspace, rank },
                    sys::pmix_proc_t {
                        nspace,
                        rank: (rank + 1) % nnodes,
                    },
                ]
            })
            .collect::<Vec<_>>();

        let results = procss.into_iter().flat_map(|procs| {
            let txs = txs.clone();
            procs.into_iter().map(move |proc| {
                let data = globals::CData::from_slice(&[proc.rank as u8]).unwrap();
                let (event, rx) = create_event(procs.to_vec(), data);
                txs[proc.rank as usize].send(event).unwrap();
                rx.map_ok(move |data| (proc.rank, data))
            })
        });

        let Either::Left((results, _)) = select(join_all(results), join_all(fences)).await else {
            panic!("expected response");
        };

        let results = results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .fold(
                HashMap::<_, HashSet<BTreeSet<_>>>::new(),
                |mut acc, (rank, (_, data))| {
                    let rank_acc: &mut _ = acc.entry(rank).or_default();
                    rank_acc.insert(data.into_iter().collect());
                    acc
                },
            );

        let expected = HashMap::from([
            (
                0,
                HashSet::from([BTreeSet::from([0, 1]), BTreeSet::from([2, 0])]),
            ),
            (
                1,
                HashSet::from([BTreeSet::from([0, 1]), BTreeSet::from([1, 2])]),
            ),
            (
                2,
                HashSet::from([BTreeSet::from([1, 2]), BTreeSet::from([2, 0])]),
            ),
        ]);
        assert_eq!(results, expected);
    }

    async fn create_bad_fence<'a>(
        discovery: &'a DirectoryPeers<'a>,
    ) -> (
        impl Future<Output = Result<(), TestError<'a>>>,
        mpsc::UnboundedSender<globals::FenceEvent>,
    ) {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let l = net::TcpListener::bind(addr).await.unwrap();
        let addr = l.local_addr().unwrap();
        discovery.register(&addr).unwrap();

        let (tx, rx) = mpsc::unbounded_channel();

        let l = TcpListenerStream::new(l)
            .try_for_each(async |mut s| {
                tokio::io::copy(&mut s, &mut tokio::io::sink()).await?;
                Ok(())
            })
            .map_err(ModexError::from);

        let events = UnboundedReceiverStream::new(rx).map(Ok).try_for_each(
            async |globals::FenceEvent { procs, .. }| {
                let peers = discovery
                    .peers(&procs, Endpoint::Fence)
                    .await
                    .map_err(ModexError::Peer)?;
                stream::iter(peers)
                    .map(Ok)
                    .try_for_each(async |peer| {
                        let s = connect_peer(&peer).await?;
                        drop(s); // Drop without writing any data to trigger an error
                        Ok(())
                    })
                    .await
            },
        );

        let result = async move {
            select(pin!(l), pin!(events))
                .map(|result| result.factor_first().0)
                .await
        };

        (result, tx)
    }

    #[tokio::test]
    async fn test_fence_err() {
        let tmpdir = TempDir::new("fence-test").unwrap();
        let discovery = DirectoryPeers::new(tmpdir.path(), 1, 2);
        let (fence, tx) = create_fence(&discovery).await;
        let (bad_fence, bad_tx) = create_bad_fence(&discovery).await;
        let procs = vec![sys::pmix_proc_t {
            rank: sys::PMIX_RANK_WILDCARD,
            nspace: [0; _],
        }];
        let (event, rx) = create_event(procs.clone(), globals::CData::from_slice(&[1]).unwrap());
        tx.send(event).unwrap();
        let event = globals::FenceEvent {
            procs,
            data: globals::CData::from_slice(&[2]).unwrap(),
            cb: globals::ModexCallback::empty(),
        };
        bad_tx.send(event).unwrap();

        let (result, Either::Left((exit, _))) =
            join(rx, select(pin!(fence), pin!(bad_fence))).await
        else {
            panic!("expected fence exit")
        };

        let (status, _) = result.unwrap();
        assert_eq!(status, sys::PMIX_ERROR);
        assert!(exit.is_err());
    }
}
