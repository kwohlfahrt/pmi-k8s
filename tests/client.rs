use std::{
    env::set_var,
    ffi::CString,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use notify::Watcher;
use tempdir::TempDir;

use mpi_k8s::pmix;

fn is_system_rdzv(p: &PathBuf) -> bool {
    p.file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("pmix.sys."))
}

fn wait_for_file(d: &Path) -> Option<PathBuf> {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(tx).unwrap();
    watcher.watch(d, notify::RecursiveMode::Recursive).unwrap();

    for res in rx {
        if let Ok(notify::Event {
            kind: notify::EventKind::Create(_),
            paths,
            ..
        }) = res
        {
            if let Some(p) = paths.iter().filter(|p| is_system_rdzv(p)).next() {
                return Some(p.to_path_buf());
            }
        }
    }

    None
}

#[test]
fn test_client() {
    let d = TempDir::new("test-client").unwrap();
    let mut p = Command::new(env!("CARGO_BIN_EXE_mock"))
        .env("TMPDIR", d.path())
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();

    let f = wait_for_file(d.path()).unwrap();

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
