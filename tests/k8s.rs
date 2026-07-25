#![allow(clippy::unwrap_used, clippy::panic)]

use std::{
    env,
    path::Path,
    process::{Command, ExitStatus, Stdio},
    time::Duration,
};

use kube::Config;

pub fn patch_kubeconfig_for_docker(config: kube::Config) -> kube::Config {
    // This is a hack to let me use the same kubeconfig (for a kind cluster)
    // inside a devcontainer.
    let is_docker_container = Path::new("/.dockerenv").exists();
    if is_docker_container {
        let tls_server_name = config
            .tls_server_name
            .or(config.cluster_url.host().map(String::from));
        let authority = if let Some(port) = config.cluster_url.port() {
            format!("host.docker.internal:{}", port)
        } else {
            "host.docker.internal".to_owned()
        };
        let mut parts = config.cluster_url.into_parts();
        parts.authority = Some(authority.try_into().unwrap());
        let cluster_url = parts.try_into().unwrap();

        kube::Config {
            cluster_url,
            tls_server_name,
            ..config
        }
    } else {
        config
    }
}

fn kubectl_cmd(config: &kube::Config) -> Command {
    let mut cmd = Command::new("kubectl");
    cmd.args(["--server", &config.cluster_url.to_string()]);
    if let Some(tls_server_name) = &config.tls_server_name {
        cmd.args(["--tls-server-name", tls_server_name]);
    }
    cmd
}

struct Kustomization<'a> {
    config: &'a Config,
    path: &'a Path,
}

impl<'a> Kustomization<'a> {
    fn new(config: &'a Config, path: &'a Path) -> Self {
        kubectl_cmd(config)
            .args(["apply", "-k", path.to_str().unwrap()])
            .stdout(Stdio::null())
            .status()
            .unwrap();

        Self { config, path }
    }
}

impl<'a> Drop for Kustomization<'a> {
    fn drop(&mut self) {
        kubectl_cmd(self.config)
            .args(["delete", "-k", self.path.to_str().unwrap()])
            .stdout(Stdio::null())
            .status()
            .unwrap();
    }
}

fn wait_for_complete(config: &Config, name: &str, timeout: Duration) -> ExitStatus {
    kubectl_cmd(config)
        .args(["wait", "--for", "condition=Complete"])
        .arg(format!("--timeout={}s", timeout.as_secs()))
        .arg(format!("jobs.batch/{}", name))
        .stdout(Stdio::null())
        .status()
        .unwrap()
}

fn logs(config: &Config, name: &str) -> ExitStatus {
    kubectl_cmd(config)
        .args(["logs", "--all-pods", "--all-containers"])
        .arg(format!("jobs.batch/{}", name))
        .status()
        .unwrap()
}

#[tokio::test]
async fn test_fence() {
    let config = patch_kubeconfig_for_docker(Config::infer().await.unwrap());
    let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("tests/kustomization/base");
    let _k = Kustomization::new(&config, &path);

    let timeout = Duration::from_mins(1);
    if !wait_for_complete(&config, "pmi-k8s-test", timeout).success() {
        logs(&config, "pmi-k8s-test");
        panic!("expected job to complete within {:?}", timeout)
    }
}

#[tokio::test]
async fn test_modex() {
    let config = patch_kubeconfig_for_docker(Config::infer().await.unwrap());
    let path =
        Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("tests/kustomization/dmodex");
    let _k = Kustomization::new(&config, &path);

    let timeout = Duration::from_mins(1);
    if !wait_for_complete(&config, "pmi-k8s-test-dmodex", timeout).success() {
        logs(&config, "pmi-k8s-test-dmodex");
        panic!("expected job to complete within {:?}", timeout)
    }
}

#[tokio::test]
async fn test_sidecar() {
    let config = patch_kubeconfig_for_docker(Config::infer().await.unwrap());
    let path =
        Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("tests/kustomization/sidecar");
    let _k = Kustomization::new(&config, &path);

    let timeout = Duration::from_secs(10);
    if !wait_for_complete(&config, "pmi-k8s-test-sidecar", timeout).success() {
        logs(&config, "pmi-k8s-test-sidecar");
        panic!("expected job to complete within {:?}", timeout)
    }
}
