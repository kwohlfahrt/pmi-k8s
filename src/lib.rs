use std::{error::Error, fmt, io, path::PathBuf};

use clap::Parser;

pub mod fence;
pub mod modex;
pub mod net;
pub mod peer;
pub mod pmix;

#[derive(Debug, thiserror::Error)]
pub enum ModexError<E: Error + fmt::Debug> {
    #[error("error in modex communication")]
    Io(#[from] io::Error),
    #[error("error from pmix server")]
    Server(#[from] pmix::PmixError),
    #[error("error in peer discovery")]
    Peer(E),
}

#[derive(Parser, Debug)]
pub struct Cli {
    #[arg(long)]
    pub nproc: u16,
    #[arg(long)]
    pub env_dir: Option<PathBuf>,
    #[arg()]
    pub command: Option<String>,
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_args() {
        let cli = Cli::try_parse_from(["pmi-k8s", "--nproc=2", "foo"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, "foo".to_owned().into());
        assert!(cli.args.is_empty());

        let cli =
            Cli::try_parse_from(["pmi-k8s", "--nproc=2", "foo", "--", "bar", "--baz"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, "foo".to_owned().into());
        assert_eq!(cli.args, ["bar", "--baz"]);

        let cli =
            Cli::try_parse_from(["pmi-k8s", "--nproc=2", "--", "foo", "bar", "--baz"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, "foo".to_owned().into());
        assert_eq!(cli.args, ["bar", "--baz"]);

        let cli = Cli::try_parse_from(["pmi-k8s", "--nproc=2", "--", "foo"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, "foo".to_owned().into());
        assert!(cli.args.is_empty());

        let cli = Cli::try_parse_from(["pmi-k8s", "--nproc=2", "--env-dir=./foo-env"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, None);
        assert!(cli.args.is_empty());
    }
}
