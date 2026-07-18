use futures::{StreamExt, TryStreamExt};
use std::{
    collections::{HashMap, HashSet},
    env, ffi, net,
    pin::pin,
};

use k8s_openapi::api::{batch::v1::Job, core::v1::Pod};
use kube::{self, Api, Client, Config, runtime::watcher};
use thiserror::Error;

use crate::{peer::Endpoint, pmix::sys};

use super::PeerDiscovery;

pub struct KubernetesPeers {
    pods: kube::Api<Pod>,
    job_name: String,
    nproc: u16,
    nnodes: u32,
    node_rank: u32,
}

const NAME_LABEL: &str = "batch.kubernetes.io/job-name";
const RANK_LABEL: &str = "batch.kubernetes.io/job-completion-index";
// TODO: Allow configuring
pub const PORT: u16 = 5000;

enum Ranks {
    Single(u32),
    Set(HashSet<u32>),
    All,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("unable to detect Kubernetes configuration")]
    KubernetesConfig(#[from] kube::config::InferConfigError),
    #[error("error performing Kubernetes operation")]
    KubernetesApi(#[from] kube::Error),
    #[error("error watching Kubernetes resources")]
    KubernetesWatch(#[from] watcher::Error),
    #[error("required environment variable not defined")]
    MissingEnv(#[from] env::VarError),
    #[error("required environment variable could not be parsed")]
    InvalidEnv(#[from] std::num::ParseIntError),
    #[error("missing expected field on Kubernetes object")]
    MissingField(&'static str),
}

impl KubernetesPeers {
    pub async fn new(nproc: u16) -> Result<Self, Error> {
        let job_name = env::var("JOB_NAME")?;
        let node_rank = env::var("JOB_COMPLETION_INDEX")?.parse()?;
        let config = kube::Config::infer().await?;
        Self::new_with_config(job_name, nproc, node_rank, config).await
    }

    async fn new_with_config(
        job_name: String,
        nproc: u16,
        node_rank: u32,
        config: Config,
    ) -> Result<Self, Error> {
        let client = Client::try_from(config)?;
        let pods = Api::<Pod>::default_namespaced(client.clone());
        let jobs = Api::<Job>::default_namespaced(client);
        let nnodes = jobs
            .get(&job_name)
            .await?
            .spec
            .and_then(|s| s.parallelism)
            .ok_or(Error::MissingField("Job:spec.parallelism"))? as u32;

        Ok(Self {
            pods,
            job_name,
            nproc,
            nnodes,
            node_rank,
        })
    }

    fn label_selector(&self, node_ranks: &Ranks) -> String {
        match node_ranks {
            Ranks::Single(node_rank) => format!(
                "{}={},{}={}",
                NAME_LABEL, self.job_name, RANK_LABEL, node_rank,
            ),
            Ranks::Set(node_ranks) => {
                let node_ranks = node_ranks.iter().map(u32::to_string).collect::<Vec<_>>();
                format!(
                    "{}={},{} in ({})",
                    NAME_LABEL,
                    self.job_name,
                    RANK_LABEL,
                    node_ranks.join(","),
                )
            }
            Ranks::All => format!("batch.kubernetes.io/job-name={}", self.job_name),
        }
    }

    fn watch_pods(
        &self,
        node_ranks: &Ranks,
    ) -> impl futures::Stream<Item = watcher::Result<(u32, net::IpAddr)>> {
        let config = watcher::Config::default().labels(&self.label_selector(node_ranks));
        let watcher = watcher::watcher(self.pods.clone(), config);

        watcher.try_filter_map(async |e| match e {
            watcher::Event::Apply(p) | watcher::Event::InitApply(p) => {
                let ip = p.status.and_then(|s| s.pod_ip).map(|ip| {
                    ip.parse::<net::IpAddr>()
                        .expect("pod had invalid IP address")
                });
                let node_rank = p.metadata.labels.and_then(|l| {
                    l.get(RANK_LABEL)
                        .map(|rank| rank.parse::<u32>().expect("pod had invalid rank label"))
                });
                Ok(node_rank.zip(ip))
            }
            _ => Ok(None),
        })
    }

    fn port(endpoint: Endpoint) -> u16 {
        match endpoint {
            Endpoint::Fence => PORT,
            Endpoint::Modex => PORT + 1,
        }
    }
}

impl PeerDiscovery for KubernetesPeers {
    type Error = Error;

    async fn peer(
        &self,
        proc: &sys::pmix_proc_t,
        endpoint: Endpoint,
    ) -> Result<net::SocketAddr, Self::Error> {
        assert!(proc.rank <= sys::PMIX_RANK_VALID);

        let node_ranks = Ranks::Single(proc.rank / (self.nproc as u32));
        let mut pod_ips = pin!(self.watch_pods(&node_ranks));
        #[allow(
            clippy::unwrap_used,
            reason = "watcher streams automatically recover from errors"
        )]
        let pod_ip = pod_ips.next().await.unwrap()?;
        Ok(net::SocketAddr::new(pod_ip.1, Self::port(endpoint)))
    }

    async fn peers(
        &self,
        procs: &[sys::pmix_proc_t],
        endpoint: Endpoint,
    ) -> Result<Vec<net::SocketAddr>, Self::Error> {
        let mut peers = HashMap::new();
        let (num_addrs, node_ranks) = if let [
            sys::pmix_proc_t {
                rank: sys::PMIX_RANK_WILDCARD,
                // TODO: Handle other namespaces
                nspace: _,
            },
        ] = procs
        {
            (self.nnodes as usize, Ranks::All)
        } else {
            let nodes = procs
                .iter()
                .map(|sys::pmix_proc_t { rank, nspace: _ }| rank / (self.nproc as u32))
                .collect::<HashSet<_>>();
            (nodes.len(), Ranks::Set(nodes))
        };
        let mut pod_ips = pin!(self.watch_pods(&node_ranks));

        while peers.len() < num_addrs {
            #[allow(
                clippy::unwrap_used,
                reason = "watcher streams automatically recover from errors"
            )]
            let (rank, pod_ip) = pod_ips.next().await.unwrap()?;
            peers.insert(rank, net::SocketAddr::new(pod_ip, Self::port(endpoint)));
        }

        Ok(peers.into_values().collect::<Vec<_>>())
    }

    fn local_ranks(&self) -> impl Iterator<Item = u32> {
        (self.node_rank * self.nproc as u32)..((self.node_rank + 1) * self.nproc as u32)
    }

    fn hostnames(&self) -> impl Iterator<Item = ffi::CString> {
        (0..self.nnodes).map(|rank| {
            #[allow(clippy::unwrap_used, reason = "Literal string without NULLs")]
            ffi::CString::new(format!("{}-{}", self.job_name, rank)).unwrap()
        })
    }

    fn node_rank(&self) -> u32 {
        self.node_rank
    }
}
