use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use tracing::Level;
use url::Url;

use crate::paths::{DetectorTemplate, InvalidPathTemplate, PathSpec, ScanTemplate, VisitTemplate};

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
    /// List the current configuration
    Info(InfoOptions),
    /// Generate the graphql schema
    Schema,
    /// Compare and/or update numtracker directories
    Sync(SyncOptions),
    /// Edit beamlines and their configurations
    Config(ConfigOptions),
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

#[derive(Debug, Parser)]
pub struct InfoOptions {
    /// Limit the info to just one beamline
    #[clap(short, long)]
    beamline: Option<String>,
}

#[derive(Debug, Parser)]
pub struct SyncOptions {
    /// Limit the update to a single beamline
    #[clap(short, long, global = true)]
    beamline: Option<String>,
    /// Mode
    #[clap(subcommand)]
    pub mode: Option<SyncMode>,
}

#[derive(Debug, Subcommand, Clone, Copy)]
pub enum SyncMode {
    /// Update the scan numbers in the DB to match those in the configured directories
    Import {
        #[clap(short, long)]
        force: bool,
    },
    /// Set the scan number files in the configured directories to match those in the DB
    Export {
        #[clap(short, long)]
        force: bool,
    },
}

// Need to be able to:
// * Add a new beamline
//   - nt config beamline --new i22
// * Add a new template (visit/scan/detector)
//   - nt config template add visit '/dls/{template}/path'
//   - nt config template add scan '{scan}/{file}/{path}'
//   - nt config template add detector '{detector}/{file}/{path}'
// * Set the scan number for a beamline
//   - nt config beamline b21 --scan-number 123
// * Set the templates used for a beamline
//   - nt config beamline b21 --visit /path/to/visit
//   - nt config beamline b21 --scan path/to/scan
//   - nt config beamline b21 --det path/to/det
#[derive(Debug, Parser)]
pub struct ConfigOptions {
    #[clap(subcommand)]
    pub action: ConfigAction,
}

#[derive(Debug, Subcommand)]
pub enum ConfigAction {
    Beamline(BeamlineConfig),
    Template(TemplateConfig),
}

#[derive(Debug, Parser)]
pub struct BeamlineConfig {
    pub beamline: String,
    #[clap(short, long)]
    pub new: bool,
    #[clap(long)]
    pub scan_number: Option<u32>,
    #[clap(long)]
    pub visit: Option<Option<String>>,
    #[clap(long)]
    pub scan: Option<Option<String>>,
    #[clap(long)]
    pub detector: Option<Option<String>>,
}

#[derive(Debug, Parser)]
pub struct TemplateConfig {
    #[clap(subcommand)]
    pub action: TemplateAction,
}

#[derive(Debug, Clone, Subcommand)]
pub enum TemplateAction {
    Add {
        kind: TemplateKind,
        template: String,
    },
    List {
        filter: Option<TemplateKind>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TemplateKind {
    Visit,
    Scan,
    Detector,
}

impl TemplateKind {
    pub fn validate(&self, template: &str) -> Result<(), InvalidPathTemplate> {
        match self {
            TemplateKind::Visit => VisitTemplate::validate(template),
            TemplateKind::Scan => ScanTemplate::validate(template),
            TemplateKind::Detector => DetectorTemplate::validate(template),
        }
    }
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

impl InfoOptions {
    pub fn beamline(&self) -> Option<&str> {
        self.beamline.as_deref()
    }
}

impl SyncOptions {
    pub fn beamline(&self) -> Option<&str> {
        self.beamline.as_deref()
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
