use std::{
    env::set_var,
    ffi::CString,
    fs::DirEntry,
    io::BufRead,
    os::unix::ffi::OsStrExt,
    path::Path,
    process::{Command, Stdio},
};

use tempdir::TempDir;

use mpi_k8s::pmix;

fn is_system_rdzv(p: &DirEntry) -> bool {
    p.file_name().to_string_lossy().starts_with("pmix.sys.")
}

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
    let rdzv_dir = stdout.lines().next().unwrap().unwrap();

    let f = std::fs::read_dir(Path::new(rdzv_dir.trim()))
        .unwrap()
        .flatten()
        .filter(is_system_rdzv)
        .next()
        .unwrap()
        .path();

    let rdzv_info = std::fs::read_to_string(f).unwrap();
    let server_url = ["PMIX_SERVER_URI41", rdzv_info.split("\n").next().unwrap()].join(";");
    let server_url = CString::new(server_url).unwrap();

    unsafe {
        set_var("PMIX_NAMESPACE", "foo");
        set_var("PMIX_RANK", "0");
    }

    let tmp_path = CString::new(d.path().as_os_str().as_bytes()).unwrap();
    let infos = [
        (pmix::sys::PMIX_TMPDIR, tmp_path.as_c_str()).into(),
        (pmix::sys::PMIX_CONNECT_TO_SYSTEM, &true).into(),
        (pmix::sys::PMIX_SERVER_URI, server_url.as_c_str()).into(),
    ];
    pmix::client_init(&infos);
    p.wait().unwrap();
}
