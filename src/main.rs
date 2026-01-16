use anyhow::Error;

mod pmix;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    // let job_name = "foo";
    // let pmix_namespace = job_name;

    println!("{:?}", pmix::get_version_str());

    Ok(())
}
