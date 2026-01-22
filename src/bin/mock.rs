use std::{ffi::CString, io::Read};

use anyhow::Error;
use mpi_k8s::pmix;

fn main() -> Result<(), Error> {
    pmix::server_init(&mut [(pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, &true).into()]);

    let namespace = &CString::new("foo").unwrap();
    pmix::register_namespace(namespace);
    pmix::register_client(namespace, 0);

    // Write the coordination directory to stdout, to signal we are ready to
    // start test clients
    println!("{}", std::env::var("TMPDIR").unwrap());

    // Wait until stdin is closed by test
    std::io::stdin().read_to_end(&mut Vec::new())?;
    Ok(())
}
