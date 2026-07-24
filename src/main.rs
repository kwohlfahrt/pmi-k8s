use futures::{
    StreamExt, TryStreamExt,
    future::{self, Either},
    stream::{self, FuturesUnordered},
};
use std::{net, pin::pin};
use tempdir::TempDir;

use anyhow::Error;
use clap::Parser;
use tokio::{fs, process::Command};

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

    let peers = KubernetesPeers::new(args.nproc).await?;
    let fence = NetFence::new(net::SocketAddr::new(WILDCARD, PORT), &peers).await?;
    let modex = NetModex::new(net::SocketAddr::new(WILDCARD, PORT + 1), &peers).await?;

    let hostnames = peers.hostnames().collect::<Vec<_>>();
    let hostname_refs = hostnames.iter().map(|h| h.as_c_str()).collect::<Vec<_>>();

    let tempdir = TempDir::new("pmi-k8s")?;
    let (s, e) = pmix::server::Server::init(tempdir.path())?;
    let ns = pmix::server::Namespace::register(&s, namespace, &hostname_refs, args.nproc)?;
    let clients = peers
        .local_ranks()
        .map(|i| pmix::server::Client::register(&ns, i))
        .collect::<Result<Vec<_>, _>>()?;

    let run = pin!(e.run(fence, modex));

    let envs = clients
        .iter()
        .map(|c| c.envs())
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(env_path) = args.env_dir {
        stream::iter(envs.iter())
            .enumerate()
            .map(Ok::<_, Error>)
            .try_for_each(async |(i, envs)| {
                let env_path = env_path.join(format!("{}.env", i));
                let mut file = fs::File::create(&env_path).await?;
                Ok(envs.write(&mut file).await?)
            })
            .await?;

        fs::File::create(env_path.join("ready")).await?;
    }

    let rcs = if let Some(command) = args.command {
        Either::Left(
            envs.into_iter()
                .map(|envs| Command::new(&command).envs(&envs).args(&args.args).spawn())
                .map(async |spawn| Ok::<_, Error>(spawn?.wait().await?))
                .collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>(),
        )
    } else {
        Either::Right(future::pending())
    };

    let rcs = match future::select(rcs, run).await {
        Either::Left((rcs, _)) => rcs?,
        Either::Right((Ok(()), rcs)) => rcs.await?,
        Either::Right((Err(err), _)) => Err(err)?,
    };

    assert!(rcs.iter().all(|rc| rc.success()));

    Ok(())
}
