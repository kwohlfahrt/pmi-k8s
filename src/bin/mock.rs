use anyhow::Error;
use mpi::traits::{Communicator, CommunicatorCollectives};

use mpi_k8s::pmix;

fn mpi() -> Result<(), Error> {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    assert_eq!(size, 2);

    let rank = world.rank();
    let mut recv = [0, size];
    world.all_gather_into(&[rank], &mut recv);
    assert_eq!(recv, [0, 1]);
    Ok(())
}

fn pmix() -> Result<(), Error> {
    // Expect the following env vars to be set:
    // - PMIX_NAMESPACE
    // - PMIX_RANK
    // - TMPDIR
    let c = pmix::client::Client::init(&[]).unwrap();
    assert!(pmix::is_initialized());
    let namespace = c.namespace().to_str().unwrap();
    assert!(!namespace.starts_with("singleton."));
    let v = c.get_job(None, pmix::sys::PMIX_JOB_SIZE);
    assert_eq!(pmix::sys::PMIX_UINT32 as u16, v.type_);
    assert_eq!(1, unsafe { v.data.uint32 });

    let v = c.get_proc(None, pmix::sys::PMIX_LOCAL_RANK);
    assert_eq!(pmix::sys::PMIX_UINT16 as u16, v.type_);
    assert_eq!(0, unsafe { v.data.rank });
    Ok(())
}

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    match args.next().unwrap().as_str() {
        "pmix" => pmix(),
        "mpi" => mpi(),
        _ => unimplemented!(),
    }
}
