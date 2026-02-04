use std::process::Command;

use tempdir::TempDir;

#[test]
fn test_client() {
    let tempdir = TempDir::new("test-pmix").unwrap();
    let program = env!("CARGO_BIN_EXE_mock");

    let mut cmd = Command::new(program);
    cmd.arg("server")
        .arg(format!("--tempdir={}", tempdir.path().to_str().unwrap()))
        .arg(format!("--nnodes={}", 1))
        .arg(format!("--nprocs={}", 1))
        .arg(format!("--node-rank={}", 0))
        .args(["--", "pmix"]);
    let mut p = cmd.spawn().unwrap();
    let rc = p.wait().unwrap();
    assert!(rc.success());
}
