use anyhow::Error;
use clap::{Args, Subcommand};
use mpi::traits::{Communicator, CommunicatorCollectives};

use pmi_k8s::pmix;

#[derive(Debug, Args)]
pub struct ClientArgs {
    #[command(subcommand)]
    command: ClientCommand,
}

#[derive(Debug, Subcommand)]
pub enum ClientCommand {
    Mpi { expected_size: i32 },
    Pmix {},
}

pub fn run(args: ClientArgs) -> Result<(), Error> {
    match args.command {
        ClientCommand::Mpi { expected_size } => mpi(expected_size),
        ClientCommand::Pmix {} => pmix(),
    }
}

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
    let v = c.get_job::<pmix::info::JobSize>(None)?;
    assert_eq!(v.get(), &4);

    let v = c.get_proc::<pmix::info::LocalRank>(None)?;
    assert!((0..4).contains(v.get()));

    let v = c.get_proc::<pmix::info::LocalSize>(None)?;
    assert_eq!(v.get(), &2);
    Ok(())
}
