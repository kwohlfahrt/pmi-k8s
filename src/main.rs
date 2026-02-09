use std::{ffi, net, pin::pin, process::Command};

use anyhow::Error;

use clap::Parser;
use futures::future::{Either, select};
use pmi_k8s::{
    fence::NetFence,
    modex::NetModex,
    peer::{KubernetesPeers, k8s::PORT},
    pmix,
};

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long)]
    nproc: u16,
    // TODO: Infer from pod spec
    #[arg(long)]
    node_rank: u32,
    // TODO: Infer from job spec
    #[arg(long)]
    nnodes: u32,
    #[arg()]
    command: String,
    #[arg(last = true)]
    args: Vec<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let args = Cli::parse();
    let namespace = c"foo";

    let peers = KubernetesPeers::new(args.nnodes).await;
    let fence = NetFence::new(
        net::SocketAddr::new("0.0.0.0".parse().unwrap(), PORT),
        &peers,
    )
    .await;
    let modex = NetModex::new(
        net::SocketAddr::new("0.0.0.0".parse().unwrap(), PORT + 1),
        &peers,
        args.nproc,
    )
    .await;

    // FIXME: Get from pod names, or remove if not needed
    let hostnames = (0..args.nnodes)
        .map(|node_rank| ffi::CString::new(format!("host-{}", node_rank)).unwrap())
        .collect::<Vec<_>>();
    let hostname_refs = hostnames.iter().map(|h| h.as_c_str()).collect::<Vec<_>>();

    let s = pmix::server::Server::init(fence, modex).unwrap();
    let ns = pmix::server::Namespace::register(&s, namespace, &hostname_refs, args.nproc);
    let clients = ((args.node_rank * args.nproc as u32)
        ..((args.node_rank + 1) * args.nproc as u32))
        .map(|i| pmix::server::Client::register(&ns, i as u32))
        .collect::<Vec<_>>();

    let ps = clients
        .iter()
        .map(|c| {
            let mut cmd = Command::new(&args.command);
            cmd.envs(&c.envs()).args(&args.args).spawn().unwrap()
        })
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

    assert!(rcs.unwrap().iter().all(|rc| rc.success()));

    Ok(())
}
