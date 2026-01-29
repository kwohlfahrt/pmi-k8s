use anyhow::Error;
use mpi::traits::{Communicator, CommunicatorCollectives};

use mpi_k8s::pmix;

mod server;
use crate::server::{server, servers};

fn mpi(expected_size: i32) -> Result<(), Error> {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    assert_eq!(size, expected_size);

    let rank = world.rank();
    let mut recv = std::iter::repeat_n(0, size as usize).collect::<Vec<_>>();
    world.all_gather_into(&[rank], &mut recv);
    assert_eq!(recv, (0..size).collect::<Vec<i32>>());
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
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let args = args.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    // TODO: Proper argument parsing
    match args.as_slice() {
        ["client", "pmix", _] => pmix(),
        ["client", "mpi", size] => mpi(size.parse().unwrap()),
        ["servers", cmd, nnodes, nprocs] => {
            servers(cmd, nnodes.parse().unwrap(), nprocs.parse().unwrap())
        }
        ["server", cmd, nnodes, nprocs, node_rank] => server(
            cmd,
            nnodes.parse().unwrap(),
            nprocs.parse().unwrap(),
            node_rank.parse().unwrap(),
        ),
        _ => unimplemented!(),
    }
}
