use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use tracing::Level;
use url::Url;

#[derive(Debug, Parser)]
pub struct Cli {
    #[clap(short, long, default_value = "numtracker.db")]
    pub(crate) db: PathBuf,
    #[clap(flatten, next_help_heading = "Logging/Debug")]
    verbose: Verbosity<InfoLevel>,
    #[clap(flatten, next_help_heading = "Tracing and Logging")]
    tracing: TracingOptions,
    #[clap(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Parser)]
pub struct TracingOptions {
    /// The URL of the tracing OTLP platform (eg Jaeger)
    #[clap(long = "tracing")]
    tracing_url: Option<Url>,
    /// The minimum level of tracing events to send
    #[clap(long, default_value_t = Level::INFO)]
    tracing_level: Level,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the server to respond to visit and scan path requests
    Serve(ServeOptions),
    // Single variant enum for now but will eventually have:
    // * Schema - generate the graphql schema
    // * Sync - importing and/or exporting scan numbers from the configured directories
    // * Config - Setting/choosing/adding path templates etc
    // * Info - Show current configuration for a beamline
}

#[derive(Debug, Parser)]
pub struct ServeOptions {
    /// The IP for this to service to be bound to
    #[clap(short = 'H', long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    host: Ipv4Addr,
    /// The port to open for requests
    #[clap(short, long, default_value_t = 8000)]
    port: u16,
}

impl Cli {
    pub fn init() -> Self {
        Self::parse()
    }
    pub fn log_level(&self) -> Option<Level> {
        use clap_verbosity_flag::Level as ClapLevel;
        self.verbose.log_level().map(|lvl| match lvl {
            ClapLevel::Error => Level::ERROR,
            ClapLevel::Warn => Level::WARN,
            ClapLevel::Info => Level::INFO,
            ClapLevel::Debug => Level::DEBUG,
            ClapLevel::Trace => Level::TRACE,
        })
    }
    pub fn tracing(&self) -> &TracingOptions {
        &self.tracing
    }
}

impl ServeOptions {
    pub(crate) fn addr(&self) -> (Ipv4Addr, u16) {
        (self.host, self.port)
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
