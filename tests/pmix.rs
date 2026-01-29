use std::process::Command;

#[test]
fn test_client() {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mock"));
    let mut p = cmd.args(["servers", "pmix", "1", "1"]).spawn().unwrap();
    assert!(p.wait().unwrap().success())
}
