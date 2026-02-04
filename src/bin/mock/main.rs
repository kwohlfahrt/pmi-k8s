use anyhow::Error;
use clap::{Parser, Subcommand};

mod client;
mod server;

#[derive(Subcommand, Debug)]
enum Commands {
    Client(client::ClientArgs),
    Server(server::ServerArgs),
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

fn main() -> Result<(), Error> {
    let args = Cli::parse();
    match args.cmd {
        Commands::Client(args) => client::run(args),
        Commands::Server(args) => server::run(args),
    }
}
