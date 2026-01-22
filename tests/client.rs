use std::{ffi::CString, path::Path, process::Command, sync::Barrier};

use tempdir::TempDir;

use mpi_k8s::pmix;

fn server(namespace: &str, tmpdir: &Path, ready: &Barrier, done: &Barrier) {
    let tmpdir = CString::new(tmpdir.to_str().unwrap()).unwrap();
    pmix::server_init(&mut [
        (pmix::sys::PMIX_SYSTEM_TMPDIR, tmpdir.as_c_str()).into(),
        (pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, &true).into(),
    ]);

    let namespace = &CString::new(namespace).unwrap();
    pmix::register_namespace(namespace);
    pmix::register_client(namespace, 0);
    ready.wait();
    done.wait();
}

#[test]
fn test_client() {
    let d = TempDir::new("test-client").unwrap();
    let ns = "foo";
    let ready = Barrier::new(2);
    let done = Barrier::new(2);

    std::thread::scope(|s| {
        let server = s.spawn(|| server(ns, d.path(), &ready, &done));
        ready.wait();

        let mut p = Command::new(env!("CARGO_BIN_EXE_mock"))
            .env("TMPDIR", d.path())
            .env("PMIX_NAMESPACE", ns)
            .env("PMIX_RANK", "0")
            .spawn()
            .unwrap();
        assert!(p.wait().unwrap().success());

        done.wait();
        server.join().unwrap();
    });
}
