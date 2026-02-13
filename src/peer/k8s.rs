use futures::{StreamExt, TryStreamExt};
use std::{collections::HashMap, env, ffi, net, pin::pin};

use k8s_openapi::api::{batch::v1::Job, core::v1::Pod};
use kube::{self, Api, Client, Config, runtime::watcher};

use super::PeerDiscovery;

pub struct KubernetesPeers {
    pods: kube::Api<Pod>,
    job_name: String,
    nnodes: u32,
    node_rank: u32,
}

const NAME_LABEL: &str = "batch.kubernetes.io/job-name";
const RANK_LABEL: &str = "batch.kubernetes.io/job-completion-index";
// TODO: Allow configuring
pub const PORT: u16 = 5000;

impl KubernetesPeers {
    pub async fn new() -> Self {
        let job_name = env::var("JOB_NAME").unwrap();
        let node_rank = env::var("JOB_COMPLETION_INDEX").unwrap().parse().unwrap();
        let config = Config::infer().await.unwrap();
        Self::new_with_config(job_name, node_rank, config).await
    }

    async fn new_with_config(job_name: String, node_rank: u32, config: Config) -> Self {
        let client = Client::try_from(config).unwrap();
        let pods = Api::<Pod>::default_namespaced(client.clone());
        let jobs = Api::<Job>::default_namespaced(client);
        let nnodes = jobs
            .get(&job_name)
            .await
            .unwrap()
            .spec
            .and_then(|s| s.parallelism)
            .unwrap() as u32;

        Self {
            pods,
            job_name,
            nnodes,
            node_rank,
        }
    }

    fn label_selector(&self, node_rank: Option<u32>) -> String {
        if let Some(node_rank) = node_rank {
            format!(
                "{}={},{}={}",
                NAME_LABEL, self.job_name, RANK_LABEL, node_rank
            )
        } else {
            format!("batch.kubernetes.io/job-name={}", self.job_name)
        }
    }

    fn watch_pods(
        &self,
        node_rank: Option<u32>,
    ) -> impl futures::Stream<Item = watcher::Result<(u32, net::IpAddr)>> {
        let config = watcher::Config::default().labels(&self.label_selector(node_rank));
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
}

impl PeerDiscovery for KubernetesPeers {
    async fn peer(&self, node_rank: u32) -> net::SocketAddr {
        let mut pod_ips = pin!(self.watch_pods(Some(node_rank)));
        let pod_ip = pod_ips.next().await.unwrap().unwrap();
        // FIXME: Hack - adding +1 here because this method is used by modex, and peers() is used by fences
        net::SocketAddr::new(pod_ip.1, PORT + 1)
    }

    async fn peers(&self) -> HashMap<u32, net::SocketAddr> {
        let mut peers = HashMap::new();
        let mut pod_ips = pin!(self.watch_pods(None));
        while peers.len() < self.nnodes as usize {
            let (rank, pod_ip) = pod_ips.next().await.unwrap().unwrap();
            peers.insert(rank, net::SocketAddr::new(pod_ip, PORT));
        }
        peers
    }

    fn local_ranks(&self, nproc: u16) -> impl Iterator<Item = u32> {
        (self.node_rank * nproc as u32)..((self.node_rank + 1) * nproc as u32)
    }

    fn hostnames(&self) -> impl Iterator<Item = ffi::CString> {
        (0..self.nnodes)
            .map(|rank| ffi::CString::new(format!("{}-{}", self.job_name, rank)).unwrap())
    }
}
