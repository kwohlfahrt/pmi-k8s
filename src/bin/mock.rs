use std::{ffi::CString, io::Read};

use anyhow::Error;
use mpi_k8s::pmix;

fn main() -> Result<(), Error> {
    println!("{:?}", pmix::get_version_str());

    pmix::server_init(&mut [(pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, &true).into()]);

    let namespace = &CString::new("foo").unwrap();
    pmix::register_namespace(namespace);
    pmix::register_client(namespace, 0);

    // Wait until stdin is closed by test
    std::io::stdin().read_to_end(&mut Vec::new())?;
    Ok(())
}
