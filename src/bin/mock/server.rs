use clap::Args;
use futures::future::{Either, select};
use std::{ffi::CString, fs, net, path::PathBuf, pin::pin, process::Command};

use anyhow::Error;

use pmi_k8s::{
    fence::NetFence,
    modex::NetModex,
    peer::{self, PeerDiscovery},
    pmix,
};

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[arg(long)]
    tempdir: PathBuf,
    #[arg(long)]
    nnodes: u32,
    #[arg(long)]
    nprocs: u16,
    #[arg(last = true)]
    command: Vec<String>,
}

fn spawn_client(args: &[String], c: &pmix::server::Client) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.arg("client")
        .args(args)
        .envs(&c.envs().unwrap())
        .spawn()
        .unwrap()
}

pub(crate) async fn run(args: ServerArgs) -> Result<(), Error> {
    let ServerArgs {
        nnodes,
        tempdir: tmpdir,
        nprocs,
        command,
    } = args;

    let namespace = "foo";

    let peer_dir = tmpdir.join("peer-discovery-fence");
    fs::create_dir_all(&peer_dir).unwrap();
    let peers = peer::DirectoryPeers::new(&peer_dir, nnodes);
    let fence = NetFence::new(
        net::SocketAddr::new(net::Ipv6Addr::LOCALHOST.into(), 0),
        &peers,
    )
    .await
    .unwrap();
    peers.register(&fence.addr()).unwrap();

    let peer_dir = tmpdir.join("peer-discovery-modex");
    fs::create_dir_all(&peer_dir).unwrap();
    let peers = peer::DirectoryPeers::new(&peer_dir, nnodes);
    let modex = NetModex::new(
        net::SocketAddr::new(net::Ipv6Addr::LOCALHOST.into(), 0),
        &peers,
        nprocs,
    )
    .await
    .unwrap();
    peers.register(&modex.addr()).unwrap();

    let server_dir = tmpdir.join("server");
    let s = pmix::server::Server::init(&server_dir).unwrap();

    let hostnames = peers.hostnames().collect::<Vec<_>>();
    let hostnames = hostnames.iter().map(|h| h.as_c_str()).collect::<Vec<_>>();
    let namespace = &CString::new(namespace).unwrap();
    let n = pmix::server::Namespace::register(&s, namespace, &hostnames, nprocs)?;
    let clients = peers
        .local_ranks(nprocs)
        .map(|i| pmix::server::Client::register(&n, i))
        .collect::<Result<Vec<_>, _>>()?;

    let ps = clients
        .iter()
        .map(|c| spawn_client(&command, c))
        .collect::<Vec<_>>();

    let rcs = tokio::task::spawn_blocking(|| {
        ps.into_iter()
            .map(|mut p| p.wait().unwrap())
            .collect::<Vec<_>>()
    });
    let run = pin!(s.run(&fence, &modex));
    let Either::Left((rcs, _)) = select(rcs, run).await else {
        panic!("server stopped unexpectedly")
    };

    assert!(rcs.unwrap().into_iter().all(|rc| rc.success()));
    Ok(())
}
