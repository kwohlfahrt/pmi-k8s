use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use mpi_k8s::coordinator::protocol::{CoordMessage, CoordServer};
use mpi_k8s::coordinator::FenceCoordinator;
use mpi_k8s::k8s::{PodDiscovery, PodIdentity};
use mpi_k8s::kv_store::KvStore;
use mpi_k8s::pmix::server::{PmixEvent, PmixServer};

/// Discovery timeout
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(300);

/// PMIx socket directory
const PMIX_TMPDIR: &str = "/tmp/pmix";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("mpi_k8s=info".parse()?))
        .init();

    info!("Starting mpi-k8s PMIx server");

    // Load pod identity from environment
    let identity = PodIdentity::from_env()?;
    let nspace = identity.pmix_nspace();

    info!(
        rank = identity.rank,
        world_size = identity.world_size,
        nspace = nspace,
        "Pod identity loaded"
    );

    // Create KV store
    let kv_store = KvStore::new();

    // Discover peer pods
    info!("Discovering peer pods...");
    let discovery = PodDiscovery::new(identity.clone()).await?;
    let peers = discovery.discover_peers(DISCOVERY_TIMEOUT).await?;
    let peers = PodDiscovery::sort_peers_by_rank(peers);

    info!(
        num_peers = peers.len(),
        "Discovered all peers, starting coordination server"
    );

    // Start coordination server
    let coord_addr = format!("0.0.0.0:{}", identity.coord_port);
    let (coord_server, mut coord_rx) = CoordServer::bind(&coord_addr).await?;

    // Spawn coordination server task
    tokio::spawn(async move {
        coord_server.run().await;
    });

    // Create fence coordinator
    let fence_coordinator = Arc::new(FenceCoordinator::new(
        identity.rank,
        identity.world_size,
        peers,
        kv_store.clone(),
    ));

    // Create channel for PMIx events
    let (pmix_tx, mut pmix_rx) = mpsc::unbounded_channel();

    // Ensure PMIx tmpdir exists
    std::fs::create_dir_all(PMIX_TMPDIR)?;

    // Initialize PMIx server
    let pmix_server = PmixServer::new(pmix_tx, kv_store.clone(), PMIX_TMPDIR)?;

    // Register namespace
    let job_info = vec![
        ("pmix.univ.size".to_string(), identity.world_size.to_string()),
        ("pmix.job.size".to_string(), identity.world_size.to_string()),
        ("pmix.local.size".to_string(), "1".to_string()),
        (
            "pmix.local.rank".to_string(),
            identity.rank.to_string(),
        ),
        ("pmix.nodeid".to_string(), identity.rank.to_string()),
    ];
    pmix_server.register_nspace(&nspace, identity.world_size, &job_info)?;

    // Register our client
    pmix_server.register_client(&nspace, identity.rank)?;

    info!("PMIx server ready, waiting for client");

    // Main event loop
    let mut clients_finalized = 0u32;

    loop {
        tokio::select! {
            // Handle PMIx events from local client
            Some(event) = pmix_rx.recv() => {
                match event {
                    PmixEvent::ClientConnected { nspace, rank } => {
                        info!(nspace, rank, "Local client connected");
                    }
                    PmixEvent::ClientFinalized { nspace, rank } => {
                        info!(nspace, rank, "Local client finalized");
                        clients_finalized += 1;
                        if clients_finalized >= 1 {
                            // With single process per pod, we exit when our client is done
                            info!("All local clients finalized, shutting down");
                            break;
                        }
                    }
                    PmixEvent::FenceRequest { request, data, callback } => {
                        let fc = fence_coordinator.clone();
                        let ns = nspace.clone();
                        tokio::spawn(async move {
                            if let Err(e) = fc.start_fence(request, data, callback, &ns).await {
                                error!(error = %e, "Fence failed");
                            }
                        });
                    }
                    PmixEvent::DirectModexRequest { nspace: ns, rank, callback } => {
                        // Try to get from local store first
                        if let Some(data) = kv_store.get_modex_data(&ns, rank) {
                            callback.complete(mpi_k8s::pmix::bindings::PMIX_SUCCESS as i32, &data);
                        } else {
                            // TODO: Fetch from remote peer
                            warn!(ns, rank, "Modex data not found locally");
                            callback.complete(mpi_k8s::pmix::bindings::PMIX_ERR_NOT_FOUND as i32, &[]);
                        }
                    }
                    PmixEvent::Abort { nspace, rank, status, message } => {
                        error!(nspace, rank, status, message, "Client aborted!");
                        std::process::exit(status);
                    }
                }
            }

            // Handle coordination messages from peer pods
            Some((msg, addr)) = coord_rx.recv() => {
                handle_coord_message(msg, addr, &fence_coordinator, &nspace);
            }

            // Shutdown signal
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
                break;
            }
        }
    }

    info!("mpi-k8s shutting down");
    drop(pmix_server);

    Ok(())
}

fn handle_coord_message(
    msg: CoordMessage,
    _addr: SocketAddr,
    fence_coordinator: &Arc<FenceCoordinator>,
    nspace: &str,
) {
    match msg {
        CoordMessage::FenceData {
            fence_id,
            rank,
            data,
        } => {
            fence_coordinator.handle_fence_data(fence_id, rank, data, nspace);
        }
        CoordMessage::FenceComplete { .. } => {
            // Handled by fence coordinator
        }
        CoordMessage::ModexRequest { .. } => {
            // TODO: Handle remote modex requests
            warn!("Remote modex requests not yet implemented");
        }
        CoordMessage::ModexResponse { .. } => {
            // TODO: Handle modex responses
        }
        CoordMessage::Ack { .. } | CoordMessage::Error { .. } => {
            // Acknowledgments
        }
    }
}
