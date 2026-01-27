use std::process::Command;

use anyhow::Error;

use mpi_k8s::pmix;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    // let job_name = "foo";
    // let pmix_namespace = job_name;

    println!("{:?}", pmix::get_version_str());

    let infos = [(pmix::sys::PMIX_SERVER_SYSTEM_SUPPORT, true).into()];
    let mut s = pmix::server::Server::init(&infos).unwrap();
    assert!(pmix::is_initialized());

    let mut ns = pmix::server::Namespace::register(&mut s, c"foobar");
    let c = pmix::server::Client::register(&mut ns, 0);

    let mut args = std::env::args().skip(1);
    let program = args.next().unwrap();
    let mut p = Command::new(program)
        .args(args)
        .envs(&c.envs())
        .spawn()
        .unwrap();
    assert!(p.wait().unwrap().success());

    Ok(())
}
