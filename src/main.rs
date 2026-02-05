use std::{net, process::Command};

use anyhow::Error;

use mpi_k8s::{fence::NetFence, modex::FileModex, peer::DirPeerDiscovery, pmix};
use tempdir::TempDir;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    // let job_name = "foo";
    // let pmix_namespace = job_name;

    println!("{:?}", pmix::get_version_str());

    let mut args = std::env::args().skip(1);
    let program = args.next().unwrap();
    let mut cmd = Command::new(program);
    let cmd = cmd.args(args);

    let tmpdir = TempDir::new("pmix-k8s").unwrap();
    let peers = DirPeerDiscovery::new(tmpdir.path(), 1);
    let fence = NetFence::new(
        net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 0),
        &peers,
    );
    let modex = FileModex::new(tmpdir.path(), 0, 1);
    let mut s = pmix::server::Server::init(fence, modex).unwrap();
    assert!(pmix::is_initialized());

    let n = 2;
    let ns = pmix::server::Namespace::register(&mut s, c"foobar", &[c"localhost"], 1);
    let clients = (0..n)
        .map(|i| pmix::server::Client::register(&ns, i as u32))
        .collect::<Vec<_>>();

    let mut ps = clients
        .iter()
        .map(|c| cmd.envs(&c.envs()).spawn().unwrap())
        .take(2)
        .collect::<Vec<_>>();

    assert!(ps.iter_mut().all(|p| p.wait().unwrap().success()));

    Ok(())
}
