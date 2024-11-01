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
use std::fmt::Display;
use std::path::{Path, PathBuf};

use async_graphql::extensions::Tracing;
use async_graphql::http::GraphiQLSource;
use async_graphql::{Context, EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Extension, Router};
use tokio::net::TcpListener;
use tracing::instrument;

use crate::cli::ServeOptions;
use crate::context::{BeamlineContext, ScanService, Subdirectory, VisitService};
use crate::db_service::SqliteScanPathService;
pub async fn serve_graphql(db: &Path, opts: ServeOptions) {
    let db = SqliteScanPathService::connect(db).await.unwrap();
    let schema = Schema::build(Query, Mutation, EmptySubscription)
        .extension(Tracing)
        .data(db)
        .finish();
    let app = Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphiql", get(graphiql))
        .layer(Extension(schema));
    let listener = TcpListener::bind(opts.addr()).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub fn graphql_schema() {
    let schema = Schema::new(Query, Mutation, EmptySubscription);
    println!("{}", schema.sdl());
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

#[instrument(skip_all)]
async fn graphql_handler(
    schema: Extension<Schema<Query, Mutation, EmptySubscription>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

/// Read-only API for GraphQL
struct Query;

/// Read-write API for GraphQL
struct Mutation;

/// GraphQL type to mimic a key-value pair from the map type that GraphQL doesn't have
#[derive(SimpleObject)]
struct DetectorPath {
    name: String,
    path: String,
}

/// GraphQL type to provide path data for a specific visit
struct VisitPath {
    service: VisitService,
}

/// GraphQL type to provide path data for the next scan for a given visit
struct ScanPaths {
    service: ScanService,
}

#[derive(Debug)]
struct NonUnicodePath;

impl NonUnicodePath {
    /// Try and convert a path to a string (via OsString), returning a NonUnicodePath
    /// error if not possible
    fn check(path: PathBuf) -> Result<String, NonUnicodePath> {
        path.into_os_string()
            .into_string()
            .map_err(|_| NonUnicodePath)
    }
}

impl Display for NonUnicodePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Path contains non-unicode characters")
    }
}

impl Error for NonUnicodePath {}

#[Object]
impl VisitPath {
    #[instrument(skip(self))]
    async fn visit(&self) -> &str {
        self.service.visit()
    }
    #[instrument(skip(self))]
    async fn beamline(&self) -> &str {
        self.service.beamline()
    }
    #[instrument(skip(self))]
    async fn directory(&self) -> async_graphql::Result<String> {
        let visit_directory = self.service.visit_directory().await?;
        Ok(NonUnicodePath::check(visit_directory)?)
    }
}

#[Object]
impl ScanPaths {
    /// Information about the visit that isn't dependent on a specific scan
    #[instrument(skip(self))]
    async fn visit(&self) -> VisitPath {
        VisitPath {
            service: self.service.as_visit_service(),
        }
    }

    /// The root scan file for this scan. The path has no extension so that the format can be
    /// chosen by the client.
    #[instrument(skip(self))]
    async fn scan_file(&self) -> async_graphql::Result<String> {
        Ok(NonUnicodePath::check(self.service.scan_file().await?)?)
    }

    /// The scan number for this scan. This should be unique for the requested beamline.
    #[instrument(skip(self))]
    async fn scan_number(&self) -> usize {
        self.service.scan_number()
    }

    /// The paths where the given detectors should write their files.
    ///
    /// Detector names are normalised before being used in file names by replacing any
    /// non-alphanumeric characters with '_'. If there are duplicate names in the list
    /// of detectors after this normalisation, there will be duplicate paths in the
    /// results.
    // TODO: The docs here reference the implementation specific behaviour in the normalisation
    #[instrument(skip(self))]
    async fn detectors(&self, names: Vec<String>) -> async_graphql::Result<Vec<DetectorPath>> {
        Ok(self
            .service
            .detector_files(&names)
            .await?
            .into_iter()
            .map(|(det, path)| {
                NonUnicodePath::check(path).map(|path| DetectorPath {
                    name: det.into(),
                    path,
                })
            })
            .collect::<Result<_, _>>()?)
    }
}

#[Object]
impl Query {
    #[instrument(skip(self, ctx))]
    async fn paths(
        &self,
        ctx: &Context<'_>,
        beamline: String,
        visit: String,
    ) -> async_graphql::Result<VisitPath> {
        let db = ctx.data::<SqliteScanPathService>()?;
        let service = VisitService::new(db.clone(), BeamlineContext::new(beamline, visit));
        Ok(VisitPath { service })
    }
}

#[Object]
impl Mutation {
    #[instrument(skip(self, ctx))]
    async fn scan<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        beamline: String,
        visit: String,
        sub: Option<String>,
    ) -> async_graphql::Result<ScanPaths> {
        let db = ctx.data::<SqliteScanPathService>()?;
        let service = VisitService::new(db.clone(), BeamlineContext::new(beamline, visit));
        let sub = Subdirectory::new(sub.unwrap_or_default())?;
        let service = service.new_scan(sub).await?;
        Ok(ScanPaths { service })
    }
}
