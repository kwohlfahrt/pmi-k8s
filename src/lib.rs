use clap::Parser;

pub mod fence;
pub mod modex;
pub mod peer;
pub mod pmix;

#[derive(Parser, Debug)]
pub struct Cli {
    #[arg(long)]
    pub nproc: u16,
    #[arg()]
    pub command: String,
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_args() {
        assert!(Cli::try_parse_from(["pmi-k8s", "--nproc=2"]).is_err());
        let cli = Cli::try_parse_from(["pmi-k8s", "--nproc=2", "foo"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, "foo");
        assert!(cli.args.is_empty());

        let cli =
            Cli::try_parse_from(["pmi-k8s", "--nproc=2", "foo", "--", "bar", "--baz"]).unwrap();
        assert_eq!(cli.nproc, 2);
        assert_eq!(cli.command, "foo");
        assert_eq!(cli.args, ["bar", "--baz"]);
    }
}
