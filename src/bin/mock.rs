use anyhow::Error;
use mpi_k8s::pmix;

fn main() -> Result<(), Error> {
    // Expect the following env vars to be set:
    // - PMIX_NAMESPACE
    // - PMIX_RANK
    // - TMPDIR
    let _c = pmix::client::Client::init(&[]).unwrap();
    assert!(pmix::is_initialized());
    Ok(())
}
