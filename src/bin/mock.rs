use anyhow::Error;
use mpi_k8s::pmix;

fn main() -> Result<(), Error> {
    // Expect the following env vars to be set:
    // - PMIX_NAMESPACE
    // - PMIX_RANK
    // - TMPDIR

    pmix::client_init(&[]);
    assert!(pmix::is_initialized());
    Ok(())
}
