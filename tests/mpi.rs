use std::process::Command;

use tempdir::TempDir;

#[test]
fn test_mpi() {
    let tempdir = TempDir::new("test-mpi").unwrap();
    let program = env!("CARGO_BIN_EXE_mock");

    let nnodes = 2;
    let nprocs = 2;
    let mut ps = (0..nnodes)
        .map(|i| {
            let expected_size = (nnodes * nprocs).to_string();

            let mut cmd = Command::new(program);
            cmd.arg("server")
                .arg(format!("--tempdir={}", tempdir.path().to_str().unwrap()))
                .arg(format!("--nnodes={}", nnodes))
                .arg(format!("--nprocs={}", nprocs))
                .arg(format!("--node-rank={}", i))
                .args(["--", "mpi", &expected_size]);
            cmd.spawn().unwrap()
        })
        .collect::<Vec<_>>();

    let rcs = ps.iter_mut().map(|p| p.wait().unwrap()).collect::<Vec<_>>();
    assert!(rcs.iter().all(|rc| rc.success()));
}
