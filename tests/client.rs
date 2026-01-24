use std::{ffi::CString, path::Path, process::Command, sync::Barrier};

use tempdir::TempDir;

use mpi_k8s::pmix;

fn server(namespace: &str, tmpdir: &Path, ready: &Barrier, done: &Barrier) {
    let tmpdir = CString::new(tmpdir.to_str().unwrap()).unwrap();
    let mut s = pmix::server::Server::init(&[
        (pmix::sys::PMIX_SYSTEM_TMPDIR, tmpdir.as_c_str()).into(),
        (pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into(),
    ])
    .unwrap();

    let namespace = &CString::new(namespace).unwrap();
    let mut n = pmix::server::Namespace::register(&mut s, namespace);
    let _c = pmix::server::Client::register(&mut n, 0);
    ready.wait();
    done.wait();
}

fn server_addr(d: &Path) -> Option<String> {
    for p in std::fs::read_dir(d).unwrap() {
        if let Ok(p) = p {
            if p.file_name().to_str().unwrap().starts_with("pmix.sys.") {
                let rdzv = std::fs::read_to_string(p.path()).unwrap();
                let line = rdzv.split("\n").next().unwrap();
                return Some(line.to_owned());
            }
        }
    }
    None
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

        let addr = server_addr(d.path()).unwrap();

        let mut p = Command::new(env!("CARGO_BIN_EXE_mock"))
            .env("TMPDIR", d.path())
            .env("PMIX_NAMESPACE", ns)
            .env("PMIX_RANK", "0")
            .env("PMIX_SERVER_URI41", addr)
            .spawn()
            .unwrap();
        let is_success = p.wait().map(|rc| rc.success());

        done.wait();
        server.join().unwrap();

        assert!(is_success.unwrap())
    });
}
