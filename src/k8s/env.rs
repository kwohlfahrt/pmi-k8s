use std::env;
use thiserror::Error;
use tracing::info;

/// Environment variable names
const POD_NAME_VAR: &str = "MPI_K8S_POD_NAME";
const POD_NAMESPACE_VAR: &str = "MPI_K8S_NAMESPACE";
const POD_IP_VAR: &str = "MPI_K8S_POD_IP";
const JOB_NAME_VAR: &str = "MPI_K8S_JOB_NAME";
const JOB_COMPLETION_INDEX_VAR: &str = "JOB_COMPLETION_INDEX";
const COORD_PORT_VAR: &str = "MPI_K8S_COORD_PORT";
const WORLD_SIZE_VAR: &str = "MPI_K8S_WORLD_SIZE";

/// Default coordination port
const DEFAULT_COORD_PORT: u16 = 5000;

/// Pod identity extracted from Kubernetes environment
#[derive(Debug, Clone)]
pub struct PodIdentity {
    /// Name of this pod
    pub pod_name: String,
    /// Kubernetes namespace
    pub namespace: String,
    /// Pod IP address
    pub pod_ip: String,
    /// Name of the Job this pod belongs to
    pub job_name: String,
    /// MPI rank derived from Job completion index
    pub rank: u32,
    /// Total number of ranks (world size)
    pub world_size: u32,
    /// Port for pod-to-pod coordination
    pub coord_port: u16,
}

impl PodIdentity {
    /// Read pod identity from environment variables
    ///
    /// Expected environment variables (typically from Kubernetes downward API):
    /// - MPI_K8S_POD_NAME: Pod name
    /// - MPI_K8S_NAMESPACE: Kubernetes namespace
    /// - MPI_K8S_POD_IP: Pod IP address
    /// - MPI_K8S_JOB_NAME: Job name (for pod discovery)
    /// - JOB_COMPLETION_INDEX: Completion index (becomes MPI rank)
    /// - MPI_K8S_WORLD_SIZE: Total number of MPI ranks
    /// - MPI_K8S_COORD_PORT: (optional) Coordination port, defaults to 5000
    pub fn from_env() -> Result<Self, EnvError> {
        let pod_name = env::var(POD_NAME_VAR).map_err(|_| EnvError::MissingVar(POD_NAME_VAR))?;
        let namespace =
            env::var(POD_NAMESPACE_VAR).map_err(|_| EnvError::MissingVar(POD_NAMESPACE_VAR))?;
        let pod_ip = env::var(POD_IP_VAR).map_err(|_| EnvError::MissingVar(POD_IP_VAR))?;
        let job_name = env::var(JOB_NAME_VAR).map_err(|_| EnvError::MissingVar(JOB_NAME_VAR))?;

        let rank: u32 = env::var(JOB_COMPLETION_INDEX_VAR)
            .map_err(|_| EnvError::MissingVar(JOB_COMPLETION_INDEX_VAR))?
            .parse()
            .map_err(|_| EnvError::InvalidValue(JOB_COMPLETION_INDEX_VAR))?;

        let world_size: u32 = env::var(WORLD_SIZE_VAR)
            .map_err(|_| EnvError::MissingVar(WORLD_SIZE_VAR))?
            .parse()
            .map_err(|_| EnvError::InvalidValue(WORLD_SIZE_VAR))?;

        let coord_port: u16 = env::var(COORD_PORT_VAR)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_COORD_PORT);

        let identity = Self {
            pod_name,
            namespace,
            pod_ip,
            job_name,
            rank,
            world_size,
            coord_port,
        };

        info!(
            pod_name = identity.pod_name,
            namespace = identity.namespace,
            rank = identity.rank,
            world_size = identity.world_size,
            "Pod identity loaded"
        );

        Ok(identity)
    }

    /// Create a pod identity for local development/testing
    pub fn for_testing(rank: u32, world_size: u32) -> Self {
        Self {
            pod_name: format!("test-pod-{}", rank),
            namespace: "default".to_string(),
            pod_ip: "127.0.0.1".to_string(),
            job_name: "test-job".to_string(),
            rank,
            world_size,
            coord_port: DEFAULT_COORD_PORT + rank as u16,
        }
    }

    /// Get the PMIx namespace string (derived from job name)
    pub fn pmix_nspace(&self) -> String {
        // PMIx namespace is limited to 255 chars
        let nspace = format!("{}.{}", self.namespace, self.job_name);
        if nspace.len() > 255 {
            nspace[..255].to_string()
        } else {
            nspace
        }
    }

    /// Get the coordination address for this pod
    pub fn coord_addr(&self) -> String {
        format!("{}:{}", self.pod_ip, self.coord_port)
    }
}

#[derive(Debug, Error)]
pub enum EnvError {
    #[error("Missing environment variable: {0}")]
    MissingVar(&'static str),
    #[error("Invalid value for environment variable: {0}")]
    InvalidValue(&'static str),
}
