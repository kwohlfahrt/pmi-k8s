use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::{io, mem};

use futures::stream;
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};

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
    npeers: usize,
    data: Option<Vec<u8>>,
    cb_npeers: Option<(globals::ModexCallback, usize)>,
}

enum ServeEvent {
    Submit {
        cb: globals::ModexCallback,
        npeers: usize,
    },
    Accept {
        data: Vec<u8>,
    },
}

impl FenceAcc {
    fn update(&mut self, event: ServeEvent) {
        match event {
            ServeEvent::Submit { cb, npeers } => self.cb_npeers = Some((cb, npeers)),
            ServeEvent::Accept { data } => {
                if let Some(ref mut acc) = self.data {
                    acc.extend(data);
                } else {
                    let _ = self.data.insert(data);
                }
                self.npeers += 1;
            }
        };
    }

    fn complete(&mut self) -> Option<(globals::ModexCallback, Vec<u8>)> {
        if let Some((cb, _)) = self.cb_npeers.take_if(|(_, npeers)| *npeers == self.npeers) {
            Some((cb, self.data.take().unwrap_or_default()))
        } else {
            None
        }
    }
}

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

    pub async fn serve(
        self,
        events: mpsc::UnboundedReceiver<globals::FenceEvent>,
    ) -> Result<(), ModexError<D::Error>> {
        let mut sequences: HashMap<Participants, Sequence> = Default::default();
        let events = UnboundedReceiverStream::new(events)
            .map(|e| {
                let participants = e.procs.iter().cloned().collect::<BTreeSet<_>>();
                let curr = sequences.entry(participants.clone()).or_default();
                let seq = *curr;
                *curr += 1;
                (FenceId(participants, seq), e)
            })
            .then(async |(id, globals::FenceEvent { procs, data, cb })| {
                let peers = self
                    .discovery
                    .peers(&procs, Endpoint::Fence)
                    .await
                    .map_err(ModexError::Peer)?;
                let npeers = peers.len();

                let header = Self::serialize_header(&id);
                Self::send(peers, header, data).await?;
                Ok((id, ServeEvent::Submit { cb, npeers }))
            });

        let conns = TcpListenerStream::new(self.listener)
            .and_then(async |mut c| {
                let id = Self::parse_header(&mut c).await?;
                let mut data = Vec::new();
                c.read_to_end(&mut data).await?;
                Ok((id, ServeEvent::Accept { data }))
            })
            .map_err(ModexError::<D::Error>::from);

        stream::select(events, conns)
            .try_fold(
                HashMap::<FenceId, FenceAcc>::new(),
                async |mut accs, (id, e)| {
                    let result = match accs.entry(id) {
                        Entry::Occupied(mut entry) => {
                            let acc = entry.get_mut();
                            acc.update(e);
                            if let Some(result) = acc.complete() {
                                entry.remove();
                                Some(result)
                            } else {
                                None
                            }
                        }
                        Entry::Vacant(entry) => {
                            let mut acc = FenceAcc::default();
                            acc.update(e);
                            if let Some(result) = acc.complete() {
                                Some(result)
                            } else {
                                entry.insert(acc);
                                None
                            }
                        }
                    };

                    if let Some((cb, data)) = result {
                        // TODO: Report failures
                        cb.call(sys::PMIX_SUCCESS as sys::pmix_status_t, data);
                    };
                    Ok(accs)
                },
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used, clippy::panic)]
    use std::{collections::HashSet, net::Ipv4Addr};

    use super::*;
    use crate::peer::DirectoryPeers;
    use futures::{
        TryFutureExt,
        future::{Either, join_all, select},
    };
    use tempdir::TempDir;
    use tokio::sync::oneshot;

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

        let results = txs.into_iter().enumerate().map(|(i, tx)| {
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
}
