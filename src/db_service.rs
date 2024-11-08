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

use std::fmt;
use std::marker::PhantomData;
use std::path::Path;

use error::ConfigurationError;
use futures::{Stream, TryStreamExt as _};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{query_as, SqlitePool};
use tracing::{info, instrument};

pub use self::error::SqliteTemplateError;
use crate::paths::{
    BeamlineField, DetectorField, DetectorTemplate, PathSpec, ScanField, ScanTemplate,
    VisitTemplate,
};
use crate::template::PathTemplate;

type SqliteTemplateResult<F> = Result<PathTemplate<F>, SqliteTemplateError>;

#[derive(Clone)]
pub struct SqliteScanPathService {
    pool: SqlitePool,
}

#[derive(Debug)]
pub struct NumtrackerConfig {
    pub directory: String,
    pub extension: String,
}

#[derive(Debug)]
struct RawPathTemplate<F>(Option<String>, PhantomData<F>);

impl<Spec> RawPathTemplate<Spec>
where
    Spec: PathSpec,
{
    fn as_template(&self) -> SqliteTemplateResult<Spec::Field> {
        let text = self
            .0
            .as_deref()
            .ok_or(SqliteTemplateError::TemplateNotSet)?;
        Ok(Spec::new_checked(text)?)
    }
}

impl<F> From<Option<String>> for RawPathTemplate<F> {
    fn from(value: Option<String>) -> Self {
        Self(value, PhantomData::default())
    }
}

#[derive(Debug)]
pub struct BeamlineConfiguration {
    name: String,
    scan_number: u32,
    visit: RawPathTemplate<VisitTemplate>,
    scan: RawPathTemplate<ScanTemplate>,
    detector: RawPathTemplate<DetectorTemplate>,
    fallback: Option<NumtrackerConfig>,
}

impl BeamlineConfiguration {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scan_number(&self) -> u32 {
        self.scan_number
    }

    pub fn fallback(&self) -> Option<&NumtrackerConfig> {
        self.fallback.as_ref()
    }

    pub fn visit(&self) -> SqliteTemplateResult<BeamlineField> {
        self.visit.as_template()
    }

    pub fn scan(&self) -> SqliteTemplateResult<ScanField> {
        self.scan.as_template()
    }

    pub fn detector(&self) -> SqliteTemplateResult<DetectorField> {
        self.detector.as_template()
    }
}

#[derive(Debug)]
struct DbBeamlineConfig {
    #[allow(unused)] // unused but allows use of 'SELECT * ...' queries
    id: i64,
    name: String,
    scan_number: i64,
    visit: Option<String>,
    scan: Option<String>,
    detector: Option<String>,
    fallback_directory: Option<String>,
    fallback_extension: Option<String>,
}

impl From<DbBeamlineConfig> for BeamlineConfiguration {
    fn from(value: DbBeamlineConfig) -> Self {
        let DbBeamlineConfig {
            id: _,
            name,
            scan_number,
            visit,
            scan,
            detector,
            fallback_directory,
            fallback_extension,
        } = value;
        let fallback = match (fallback_directory, fallback_extension) {
            (None, None) => None,
            (None, Some(_)) => None,
            (Some(dir), None) => Some(NumtrackerConfig {
                directory: dir,
                extension: name.clone(),
            }),
            (Some(dir), Some(ext)) => Some(NumtrackerConfig {
                directory: dir,
                extension: ext,
            }),
        };
        Self {
            name,
            scan_number: scan_number as u32,
            visit: visit.into(),
            scan: scan.into(),
            detector: detector.into(),
            fallback,
        }
    }
}

impl SqliteScanPathService {
    #[instrument]
    pub async fn connect(filename: &Path) -> Result<Self, sqlx::Error> {
        info!("Connecting to SQLite DB");
        let opts = SqliteConnectOptions::new()
            .create_if_missing(true)
            .filename(filename);
        let pool = SqlitePool::connect_with(opts).await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn current_configuration<'bl>(
        &self,
        beamline: &'bl str,
    ) -> Result<BeamlineConfiguration, ConfigurationError<'bl>> {
        query_as!(
            DbBeamlineConfig,
            "SELECT * FROM beamline WHERE name = ?",
            beamline
        )
        .fetch_optional(&self.pool)
        .await?
        .map(BeamlineConfiguration::from)
        .ok_or(ConfigurationError::MissingBeamline(beamline))
    }

    pub async fn next_scan_configuration<'bl>(
        &self,
        beamline: &'bl str,
        current_high: Option<u32>,
    ) -> Result<BeamlineConfiguration, ConfigurationError<'bl>> {
        let exp = current_high.unwrap_or(0);
        query_as!(
            DbBeamlineConfig,
            "UPDATE beamline SET scan_number = max(scan_number, ?) + 1 WHERE name = ? RETURNING *",
            exp,
            beamline
        )
        .fetch_optional(&self.pool)
        .await?
        .map(BeamlineConfiguration::from)
        .ok_or(ConfigurationError::MissingBeamline(beamline))
    }

    pub fn all_beamlines(&self) -> impl Stream<Item = sqlx::Result<BeamlineConfiguration>> + '_ {
        query_as!(DbBeamlineConfig, "SELECT * FROM beamline")
            .fetch(&self.pool)
            .map_ok(BeamlineConfiguration::from)
    }

    pub async fn update_beamline(&self, info: BeamlineConfiguration) -> sqlx::Result<()> {
        todo!()
    }

    #[cfg(test)]
    async fn ro_memory() -> Self {
        let db = Self::memory().await;
        db.pool
            .set_connect_options(SqliteConnectOptions::new().read_only(true));
        db
    }

    #[cfg(test)]
    async fn memory() -> Self {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        Self { pool }
    }
}

impl fmt::Debug for SqliteScanPathService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // This is a bit misleading when the 'db' field doesn't exist but is the most useful
        // information when debugging the state of the service
        f.debug_struct("SqliteScanPathService")
            .field("db", &self.pool.connect_options().get_filename())
            .finish()
    }
}

mod error {
    use std::error::Error;
    use std::fmt::{self, Display};

    use crate::paths::InvalidPathTemplate;

    /// Something that went wrong in the chain of querying the database for a template and
    /// converting it into a usable template.
    #[derive(Debug)]
    pub enum SqliteTemplateError {
        /// The template could not be parsed into a valid [`PathTemplate`].
        Invalid(InvalidPathTemplate),
        /// The requested template is not set for the requested beamline
        TemplateNotSet,
    }

    impl Display for SqliteTemplateError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::TemplateNotSet => f.write_str("No template set for beamline"),
                Self::Invalid(e) => write!(f, "Template is not valid: {e}"),
            }
        }
    }

    impl Error for SqliteTemplateError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                SqliteTemplateError::Invalid(e) => Some(e),
                SqliteTemplateError::TemplateNotSet => None,
            }
        }
    }

    impl From<InvalidPathTemplate> for SqliteTemplateError {
        fn from(err: InvalidPathTemplate) -> Self {
            Self::Invalid(err)
        }
    }

    #[derive(Debug)]
    pub enum ConfigurationError<'bl> {
        MissingBeamline(&'bl str),
        Db(sqlx::Error),
    }

    impl Display for ConfigurationError<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                ConfigurationError::MissingBeamline(bl) => {
                    write!(f, "No configuration available for beamline {bl:?}")
                }
                ConfigurationError::Db(e) => write!(f, "Error reading configuration: {e}"),
            }
        }
    }

    impl Error for ConfigurationError<'_> {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                ConfigurationError::MissingBeamline(_) => None,
                ConfigurationError::Db(e) => Some(e),
            }
        }
    }

    impl From<sqlx::Error> for ConfigurationError<'_> {
        fn from(value: sqlx::Error) -> Self {
            Self::Db(value)
        }
    }
}

#[cfg(test)]
mod db_tests {
    use rstest::{fixture, rstest};
    use tokio::test;

    use super::SqliteScanPathService;
    use crate::db_service::error::ConfigurationError;

    #[fixture]
    async fn db() -> SqliteScanPathService {
        SqliteScanPathService::memory().await
    }

    /// Remove repeated .await.unwrap() noise from tests
    macro_rules! ok {
        ($call:expr) => {
            $call.await.unwrap()
        };
    }
    /// Remove repeated .await.unwrap_err() noise from tests
    macro_rules! err {
        ($call:expr) => {
            $call.await.unwrap_err()
        };
    }

    #[rstest]
    #[test]
    async fn empty_db_has_no_config(#[future(awt)] db: SqliteScanPathService) {
        let e = err!(db.current_configuration("i22"));
        let ConfigurationError::MissingBeamline("i22") = e else {
            panic!("Unexpected error from missing beamline: {e}");
        };
    }
}
