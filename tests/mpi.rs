use std::{
    ffi::CString,
    path::Path,
    process::Command,
    sync::{Barrier, mpsc},
};

use tempdir::TempDir;

use mpi_k8s::pmix;

fn server(
    namespace: &str,
    tmpdir: &Path,
    ready: mpsc::Sender<Vec<pmix::env::EnvVars>>,
    done: &Barrier,
) {
    let tmpdir = CString::new(tmpdir.to_str().unwrap()).unwrap();
    let mut s = pmix::server::Server::init(&[
        (pmix::sys::PMIX_SYSTEM_TMPDIR, tmpdir.as_c_str()).into(),
        (pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into(),
    ])
    .unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let n = pmix::server::Namespace::register(&mut s, namespace, 2);
    let clients = (0..2)
        .into_iter()
        .map(|i| pmix::server::Client::register(&n, i))
        .collect::<Vec<_>>();
    ready
        .send(clients.iter().map(|c| c.envs()).collect())
        .unwrap();
    done.wait();
}

#[test]
fn test_mpi() {
    let d = TempDir::new("test-mpi").unwrap();
    let ns = "foo";
    let done = Barrier::new(2);

    std::thread::scope(|s| {
        let (tx, rx) = mpsc::channel();
        let server = s.spawn(|| server(ns, d.path(), tx, &done));
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mock"));
        let cmd = cmd.arg("mpi");

        let mut ps = rx
            .recv()
            .unwrap()
            .iter()
            .map(|envs| cmd.envs(envs).spawn().unwrap())
            .collect::<Vec<_>>();

        let rcs = ps.iter_mut().map(|p| p.wait().unwrap()).collect::<Vec<_>>();

        done.wait();
        server.join().unwrap();

        assert!(rcs.iter().all(|r| r.success()));
    });
}
