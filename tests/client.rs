use std::{
    ffi::CString,
    path::Path,
    process::Command,
    sync::{Barrier, mpsc},
};

use tempdir::TempDir;

use mpi_k8s::pmix;

fn server(namespace: &str, tmpdir: &Path, ready: mpsc::Sender<pmix::env::EnvVars>, done: &Barrier) {
    let tmpdir = CString::new(tmpdir.to_str().unwrap()).unwrap();
    let mut s = pmix::server::Server::init(&[
        (pmix::sys::PMIX_SYSTEM_TMPDIR, tmpdir.as_c_str()).into(),
        (pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into(),
    ])
    .unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let mut n = pmix::server::Namespace::register(&mut s, namespace, 1);
    let c = pmix::server::Client::register(&mut n, 0);
    ready.send(c.envs()).unwrap();
    done.wait();
}

#[test]
fn test_client() {
    let d = TempDir::new("test-client").unwrap();
    let ns = "foo";
    let done = Barrier::new(2);

    std::thread::scope(|s| {
        let (tx, rx) = mpsc::channel();
        let server = s.spawn(|| server(ns, d.path(), tx, &done));

        let mut p = Command::new(env!("CARGO_BIN_EXE_mock"))
            .envs(&rx.recv().unwrap())
            .spawn()
            .unwrap();
        let is_success = p.wait().map(|rc| rc.success());

        done.wait();
        server.join().unwrap();

        assert!(is_success.unwrap())
    });
}
