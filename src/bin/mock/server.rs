use std::{ffi::CString, path::Path, process::Command, thread, time::Duration};
use tempdir::TempDir;

use anyhow::Error;

use mpi_k8s::{fence::FileFence, pmix};

fn spawn_client(
    subcommand: &str,
    nnodes: u32,
    nprocs: u16,
    c: &pmix::server::Client,
) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.arg("client")
        .arg(subcommand)
        .arg((nnodes * nprocs as u32).to_string())
        .envs(&c.envs())
        .spawn()
        .unwrap()
}

pub(crate) fn server(
    cmd: &str,
    tmpdir: &Path,
    nnodes: u32,
    nprocs: u16,
    node_rank: u32,
) -> Result<(), Error> {
    let namespace = "foo";
    let _hostname = CString::new(format!("host-{}", node_rank)).unwrap();
    let fence = FileFence::new(tmpdir, nnodes, node_rank);
    let s = pmix::server::Server::init(fence).unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let n =
        pmix::server::Namespace::register(&s, namespace, node_rank, nprocs, nprocs as u32 * nnodes);
    let clients = ((node_rank * nprocs as u32)..((node_rank + 1) * nprocs as u32))
        .into_iter()
        .map(|i| pmix::server::Client::register(&n, i))
        .collect::<Vec<_>>();

    let mut ps = clients
        .iter()
        .map(|c| spawn_client(cmd, nnodes, nprocs, c))
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

fn spawn_server(
    subcommand: &str,
    tmpdir: &Path,
    nnodes: u32,
    nprocs: u32,
    node_rank: u32,
) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    let tmpdir = tmpdir.to_str().unwrap();
    let nnodes = nnodes.to_string();
    let nprocs = nprocs.to_string();
    let node_rank = node_rank.to_string();
    cmd.args(["server", subcommand, tmpdir, &nnodes, &nprocs, &node_rank])
        .spawn()
        .unwrap()
}

pub(crate) fn servers(cmd: &str, nnodes: u32, nprocs: u32) -> Result<(), Error> {
    let tmpdir = TempDir::new("pmix-servers").unwrap();

    let mut ps = (0..nnodes)
        .map(|rank| spawn_server(cmd, tmpdir.path(), nnodes, nprocs, rank))
        .collect::<Vec<_>>();

    assert!(
        ps.iter_mut()
            .map(|p| p.wait().unwrap())
            .all(|rc| rc.success())
    );
    Ok(())
}
