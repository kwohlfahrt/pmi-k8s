use std::{ffi::CString, process::Command};
use tempdir::TempDir;

use anyhow::Error;

use mpi_k8s::pmix;

fn spawn_client(
    subcommand: &str,
    nnodes: u32,
    nprocs: u32,
    c: &pmix::server::Client,
) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.arg("client")
        .arg(subcommand)
        .arg((nnodes * nprocs).to_string())
        .envs(&c.envs())
        .spawn()
        .unwrap()
}

pub(crate) fn server(cmd: &str, nnodes: u32, nprocs: u32, node_rank: u32) -> Result<(), Error> {
    let tmpdir = TempDir::new("pmix-server").unwrap();
    let namespace = "foo";
    let tmpdir = CString::new(tmpdir.path().as_os_str().as_encoded_bytes()).unwrap();
    let mut s = pmix::server::Server::init(&[
        (pmix::sys::PMIX_SERVER_TMPDIR, tmpdir.as_c_str()).into(),
        (pmix::sys::PMIX_SYSTEM_TMPDIR, tmpdir.as_c_str()).into(),
        (pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into(),
    ])
    .unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let n = pmix::server::Namespace::register(&mut s, namespace, nprocs, nprocs * nnodes);
    let clients = ((node_rank * nprocs)..((node_rank + 1) * nprocs))
        .into_iter()
        .map(|i| pmix::server::Client::register(&n, i))
        .collect::<Vec<_>>();

    let mut ps = clients
        .iter()
        .map(|c| spawn_client(cmd, nnodes, nprocs, c))
        .collect::<Vec<_>>();

    assert!(
        ps.iter_mut()
            .map(|p| p.wait().unwrap())
            .all(|rc| rc.success())
    );
    Ok(())
}

fn spawn_server(subcommand: &str, nnodes: u32, nprocs: u32, node_rank: u32) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    let nnodes = format!("{}", nnodes);
    let nprocs = format!("{}", nprocs);
    let node_rank = format!("{}", node_rank);
    cmd.args(["server", subcommand, &nnodes, &nprocs, &node_rank])
        .spawn()
        .unwrap()
}

pub(crate) fn servers(cmd: &str, nnodes: u32, nprocs: u32) -> Result<(), Error> {
    let mut ps = (0..nnodes)
        .map(|rank| spawn_server(cmd, nnodes, nprocs, rank))
        .collect::<Vec<_>>();

    assert!(
        ps.iter_mut()
            .map(|p| p.wait().unwrap())
            .all(|rc| rc.success())
    );
    Ok(())
}
