use clap::Args;
use futures::future::{Either, select};
use std::{ffi::CString, fs, net, path::PathBuf, pin::pin, process::Command};

use anyhow::Error;

use mpi_k8s::{fence::NetFence, modex::NetModex, peer::dir::PeerDiscovery, pmix};

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[arg(long)]
    tempdir: PathBuf,
    #[arg(long)]
    node_rank: u32,
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
        .envs(&c.envs())
        .spawn()
        .unwrap()
}

pub(crate) async fn run(args: ServerArgs) -> Result<(), Error> {
    let ServerArgs {
        nnodes,
        tempdir: tmpdir,
        node_rank,
        nprocs,
        command,
    } = args;

    let namespace = "foo";
    let hostnames = (0..nnodes)
        .map(|node_rank| CString::new(format!("host-{}", node_rank)).unwrap())
        .collect::<Vec<_>>();
    let hostnames = hostnames.iter().map(|h| h.as_c_str()).collect::<Vec<_>>();

    let peer_dir = tmpdir.join("peer-discovery-fence");
    fs::create_dir_all(&peer_dir).unwrap();
    let peers = PeerDiscovery::new(&peer_dir, nnodes);
    let fence = NetFence::new(
        net::SocketAddr::new(net::Ipv6Addr::LOCALHOST.into(), 0),
        &peers,
    )
    .await;
    peers.register(&fence.addr(), node_rank);

    let peer_dir = tmpdir.join("peer-discovery-modex");
    fs::create_dir_all(&peer_dir).unwrap();
    let peers = PeerDiscovery::new(&peer_dir, nnodes);
    let modex = NetModex::new(
        net::SocketAddr::new(net::Ipv6Addr::LOCALHOST.into(), 0),
        &peers,
        nprocs,
    )
    .await;
    peers.register(&modex.addr(), node_rank);
    let s = pmix::server::Server::init(fence, modex).unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let n = pmix::server::Namespace::register(&s, namespace, &hostnames, nprocs);
    let clients = ((node_rank * nprocs as u32)..((node_rank + 1) * nprocs as u32))
        .into_iter()
        .map(|i| pmix::server::Client::register(&n, i))
        .collect::<Vec<_>>();

    let ps = clients
        .iter()
        .map(|c| spawn_client(&command, c))
        .collect::<Vec<_>>();

    let rcs = tokio::task::spawn_blocking(|| {
        ps.into_iter()
            .map(|mut p| p.wait().unwrap())
            .collect::<Vec<_>>()
    });
    let run = pin!(s.run());
    let Either::Left((rcs, _)) = select(rcs, run).await else {
        panic!("server stopped unexpectedly")
    };

    assert!(rcs.unwrap().into_iter().all(|rc| rc.success()));
    Ok(())
}
