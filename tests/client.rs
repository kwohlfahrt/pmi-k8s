use std::{
    env::set_var,
    io::BufRead,
    process::{Command, Stdio},
};

use tempdir::TempDir;

use mpi_k8s::pmix;

#[test]
fn test_client() {
    let d = TempDir::new("test-client").unwrap();
    let mut p = Command::new(env!("CARGO_BIN_EXE_mock"))
        .env("TMPDIR", d.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = std::io::BufReader::new(p.stdout.as_mut().unwrap());
    let coord_dir = stdout.lines().next().unwrap().unwrap();

    unsafe {
        set_var("PMIX_NAMESPACE", "foo");
        set_var("PMIX_RANK", "0");
        set_var("TMPDIR", coord_dir.trim());
    }

    pmix::client_init(&[]);
    p.wait().unwrap();
}
