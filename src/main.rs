use futures::{
    TryStreamExt,
    future::{Either, select},
    stream::FuturesUnordered,
};
use std::{net, pin::pin};
use tempdir::TempDir;

use anyhow::Error;
use clap::Parser;
use tokio::process::Command;

use pmi_k8s::{
    Cli,
    fence::NetFence,
    modex::NetModex,
    peer::{KubernetesPeers, PeerDiscovery, k8s::PORT},
    pmix,
};

const WILDCARD: net::IpAddr = net::IpAddr::V4(net::Ipv4Addr::new(0, 0, 0, 0));

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let args = Cli::parse();
    let namespace = c"foo";

    let peers = KubernetesPeers::new().await?;
    let fence = NetFence::new(net::SocketAddr::new(WILDCARD, PORT), &peers).await?;
    let modex = NetModex::new(net::SocketAddr::new(WILDCARD, PORT + 1), &peers, args.nproc).await?;

    let hostnames = peers.hostnames().collect::<Vec<_>>();
    let hostname_refs = hostnames.iter().map(|h| h.as_c_str()).collect::<Vec<_>>();

    let tempdir = TempDir::new("pmi-k8s")?;
    let s = pmix::server::Server::init(fence, modex, tempdir.path())?;
    let ns = pmix::server::Namespace::register(&s, namespace, &hostname_refs, args.nproc)?;
    let clients = peers
        .local_ranks(args.nproc)
        .map(|i| pmix::server::Client::register(&ns, i))
        .collect::<Result<Vec<_>, _>>()?;

    let run = pin!(s.run());
    let rcs = clients
        .iter()
        .map(async |c| -> Result<_, Error> {
            let envs = c.envs()?;
            let mut cmd = Command::new(&args.command);
            Ok(cmd.envs(&envs).args(&args.args).spawn()?.wait().await?)
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>();

    let rcs = match select(rcs, run).await {
        Either::Left((rcs, _)) => rcs?,
        Either::Right((Err(err), _)) => Err(err)?,
    };

    assert!(rcs.iter().all(|rc| rc.success()));

    Ok(())
}
