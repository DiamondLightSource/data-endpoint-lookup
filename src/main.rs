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

use std::error::Error;

use cli::{Cli, Command};
use tracing::debug;

mod cli;
mod db_service;
mod graphql;
mod info;
mod logging;
mod numtracker;
mod paths;
mod template;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::init();
    let _ = logging::init_logging(args.log_level(), args.tracing());
    debug!(args = format_args!("{:#?}", args));
    match args.command {
        Command::Serve(opts) => graphql::serve_graphql(&args.db, opts).await,
        Command::Schema => graphql::graphql_schema(),
        Command::Info(info) => info::list_info(&args.db, info.beamline()).await,
    }
    Ok(())
}
