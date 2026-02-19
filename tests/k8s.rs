#![allow(clippy::unwrap_used, clippy::panic)]

use std::{
    env,
    path::Path,
    process::{Command, ExitStatus},
    time::Duration,
};

struct Kustomization<'a> {
    path: &'a Path,
}

impl<'a> Kustomization<'a> {
    fn new(path: &'a Path) -> Self {
        Command::new("kubectl")
            .args(["apply", "-k", path.to_str().unwrap()])
            .status()
            .unwrap();

        Self { path }
    }
}

impl<'a> Drop for Kustomization<'a> {
    fn drop(&mut self) {
        Command::new("kubectl")
            .args(["delete", "-k", self.path.to_str().unwrap()])
            .status()
            .unwrap();
    }
}

fn wait_for_complete(name: &str, timeout: Duration) -> ExitStatus {
    Command::new("kubectl")
        .args(["wait", "--for", "condition=Complete"])
        .arg(format!("--timeout={}s", timeout.as_secs()))
        .arg(format!("jobs.batch/{}", name))
        .status()
        .unwrap()
}

#[tokio::test]
async fn test_fence() {
    let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("tests/kustomization/base");
    let _k = Kustomization::new(&path);

    assert!(wait_for_complete("pmi-k8s-test", Duration::from_mins(1)).success())
}

#[tokio::test]
async fn test_modex() {
    let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("tests/kustomization/dmodex");
    let _k = Kustomization::new(&path);

    assert!(wait_for_complete("pmi-k8s-test-dmodex", Duration::from_mins(1)).success())
}
