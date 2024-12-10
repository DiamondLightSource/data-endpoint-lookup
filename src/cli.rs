// Copyright 2024 Diamond Light Source
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};
use tracing::Level;
use url::Url;

#[derive(Debug, Parser)]
pub struct Cli {
    #[clap(short, long, default_value = "numtracker.db", env = "NUMTRACKER_DB")]
    pub(crate) db: PathBuf,
    #[clap(flatten, next_help_heading = "Logging/Debug")]
    verbose: Verbosity,
    #[clap(flatten, next_help_heading = "Tracing and Logging")]
    tracing: TracingOptions,
    #[clap(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Parser)]
pub struct TracingOptions {
    /// The URL of the tracing OTLP platform (eg Jaeger)
    #[clap(long = "tracing", env = "NUMTRACKER_TRACING")]
    tracing_url: Option<Url>,
    /// The minimum level of tracing events to send
    #[clap(long, default_value_t = Level::INFO, env = "NUMTRACKER_TRACING_LEVEL")]
    tracing_level: Level,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the server to respond to visit and scan path requests
    Serve(ServeOptions),
    /// Generate the graphql schema
    Schema,
}

#[derive(Debug, Parser)]
pub struct ServeOptions {
    /// The IP for this to service to be bound to
    #[clap(short = 'H', long, default_value_t = Ipv4Addr::UNSPECIFIED, env="NUMTRACKER_HOST")]
    host: Ipv4Addr,
    /// The port to open for requests
    #[clap(short, long, default_value_t = 8000, env = "NUMTRACKER_PORT")]
    port: u16,
    /// The root directory for external number tracking
    #[clap(long, env = "NUMTRACKER_ROOT_DIRECTORY")]
    root_directory: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct Verbosity {
    /// Increase the level of logs written to stderr
    #[clap(short, long, global = true, action = ArgAction::Count)]
    verbose: u8,
    /// Disable all output to stderr/stdout
    #[clap(short, long, global = true, conflicts_with = "verbose")]
    quiet: bool,
}

impl Cli {
    pub fn init() -> Self {
        Self::parse()
    }
    pub fn tracing(&self) -> &TracingOptions {
        &self.tracing
    }
    pub fn log_level(&self) -> Option<Level> {
        self.verbose.log_level()
    }
}
impl Verbosity {
    pub fn log_level(&self) -> Option<Level> {
        if self.quiet {
            return None;
        }
        match self.verbose {
            0 => match env::var("NUMTRACKER_LOG_LEVEL")
                .map(|lvl| lvl.to_ascii_lowercase())
                .as_deref()
            {
                Ok("info") => Level::INFO,
                Ok("debug") => Level::DEBUG,
                Ok("trace") => Level::TRACE,
                Ok("warn") => Level::WARN,
                Ok("error") => Level::ERROR,
                _ => Level::ERROR,
            },
            1 => Level::INFO,
            2 => Level::DEBUG,
            _ => Level::TRACE,
        }
        .into()
    }
}

impl ServeOptions {
    pub(crate) fn addr(&self) -> (Ipv4Addr, u16) {
        (self.host, self.port)
    }
    pub(crate) fn root_directory(&self) -> Option<PathBuf> {
        self.root_directory.clone()
    }
}

impl TracingOptions {
    pub(crate) fn tracing_url(&self) -> Option<Url> {
        self.tracing_url.clone()
    }

    pub(crate) fn level(&self) -> Level {
        self.tracing_level
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::error::ErrorKind;
    use clap::Parser;
    use tracing::Level;

    use super::Cli;
    use crate::cli::Command;
    const APP: &str = "numtracker";

    #[test]
    fn serve_defaults() {
        let cli = Cli::try_parse_from([APP, "serve"]).unwrap();
        assert_eq!(cli.db, PathBuf::from("numtracker.db"));
        assert_eq!(cli.verbose.log_level(), Some(Level::ERROR));

        assert_eq!(cli.tracing().tracing_url(), None);
        assert_eq!(cli.tracing().level(), Level::INFO);

        let Command::Serve(cmd) = cli.command else {
            panic!("Unexpected subcommand: {:?}", cli.command);
        };
        assert_eq!(cmd.addr(), ("0.0.0.0".parse().unwrap(), 8000));
        assert_eq!(cmd.root_directory(), None);
    }

    #[test]
    fn global_verbose() {
        let cli = Cli::try_parse_from([APP, "-vv", "serve"]).unwrap();
        assert_eq!(cli.log_level(), Some(Level::DEBUG));

        let cli = Cli::try_parse_from([APP, "serve", "-v"]).unwrap();
        assert_eq!(cli.log_level(), Some(Level::INFO));

        let cli = Cli::try_parse_from([APP, "schema", "-q"]).unwrap();
        assert_eq!(cli.log_level(), None);
    }

    #[test]
    fn exclusive_quiet_verbose() {
        let err = Cli::try_parse_from([APP, "schema", "-q", "-v"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn max_verbosity() {
        let cli = Cli::try_parse_from([APP, "-vvv", "serve"]).unwrap();
        assert_eq!(cli.log_level(), Some(Level::TRACE));

        // Adding more flags does nothing but isn't an error
        let cli = Cli::try_parse_from([APP, "-vvvv", "serve"]).unwrap();
        assert_eq!(cli.log_level(), Some(Level::TRACE));

        let cli = Cli::try_parse_from([APP, "-vvvvv", "serve"]).unwrap();
        assert_eq!(cli.log_level(), Some(Level::TRACE));
    }

    #[test]
    fn tracing_opts() {
        let cli = Cli::try_parse_from([APP, "--tracing", "https://tracing.example.com", "serve"])
            .unwrap();
        assert_eq!(
            cli.tracing().tracing_url(),
            Some("https://tracing.example.com".parse().unwrap())
        );
        assert_eq!(cli.tracing().level(), Level::INFO);

        let cli = Cli::try_parse_from([
            APP,
            "--tracing",
            "https://tracing.example.com",
            "--tracing-level",
            "DEBUG",
            "serve",
        ])
        .unwrap();
        assert_eq!(
            cli.tracing().tracing_url(),
            Some("https://tracing.example.com".parse().unwrap())
        );
        assert_eq!(cli.tracing().level(), Level::DEBUG);
    }

    #[test]
    fn schema_command() {
        let cli = Cli::try_parse_from([APP, "schema"]).unwrap();
        let Command::Schema = cli.command else {
            panic!("Unexpected command: {:?}", cli.command);
        };
    }
}
