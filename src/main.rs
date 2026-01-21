use anyhow::Error;

use mpi_k8s::pmix;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    // let job_name = "foo";
    // let pmix_namespace = job_name;

    println!("{:?}", pmix::get_version_str());

    pmix::server_init(&[(pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, &true).into()]);
    assert!(pmix::is_initialized());
    Ok(())
}
