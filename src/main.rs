use std::ffi::CStr;

use anyhow::Error;

mod pmix;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    // let job_name = "foo";
    // let pmix_namespace = job_name;

    println!("{:?}", pmix::get_version_str());

    let tool_support = CStr::from_bytes_with_nul(pmix::sys::PMIX_SERVER_TOOL_SUPPORT).unwrap();
    pmix::server_init(&mut [(tool_support, &false).into()]);
    Ok(())
}
