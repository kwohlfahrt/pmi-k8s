use std::process::Command;

#[test]
fn test_mpi() {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mock"));
    // TODO: Change nnodes to >= 2
    let mut p = cmd.args(["servers", "mpi", "2", "2"]).spawn().unwrap();
    assert!(p.wait().unwrap().success())
}
