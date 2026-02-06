use clap::Args;
use std::{ffi::CString, fs, net, path::PathBuf, process::Command, thread, time::Duration};

use anyhow::Error;

use mpi_k8s::{fence::NetFence, modex::FileModex, peer::DirPeerDiscovery, pmix};

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

pub(crate) fn run(args: ServerArgs) -> Result<(), Error> {
    let ServerArgs {
        nnodes,
        tempdir: tmpdir,
        node_rank,
        nprocs,
        command,
    } = args;
    let peer_dir = tmpdir.join("peer-discovery");
    fs::create_dir_all(&peer_dir).unwrap();

    let namespace = "foo";
    let hostnames = (0..nnodes)
        .map(|node_rank| CString::new(format!("host-{}", node_rank)).unwrap())
        .collect::<Vec<_>>();
    let hostnames = hostnames.iter().map(|h| h.as_c_str()).collect::<Vec<_>>();
    let peers = DirPeerDiscovery::new(&peer_dir, nnodes);
    let fence = NetFence::new(
        net::SocketAddr::new(net::Ipv6Addr::LOCALHOST.into(), 0),
        &peers,
    );
    peers.register(&fence.addr(), node_rank);
    let modex = FileModex::new(&tmpdir, node_rank, nprocs);
    let s = pmix::server::Server::init(fence, modex).unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let n = pmix::server::Namespace::register(&s, namespace, &hostnames, nprocs);
    let clients = ((node_rank * nprocs as u32)..((node_rank + 1) * nprocs as u32))
        .into_iter()
        .map(|i| pmix::server::Client::register(&n, i))
        .collect::<Vec<_>>();

    let mut ps = clients
        .iter()
        .map(|c| spawn_client(&command, c))
        .collect::<Vec<_>>();

    let mut rc = Vec::with_capacity(ps.len());
    thread::scope(|scope| {
        let t = scope.spawn(|| rc.extend(ps.iter_mut().map(|p| p.wait())));
        while !t.is_finished() {
            s.handle_event(Duration::from_millis(250));
        }
    });

    assert!(rc.into_iter().all(|rc| rc.is_ok_and(|rc| rc.success())));
    Ok(())
}
