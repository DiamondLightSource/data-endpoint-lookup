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

use error::{ConfigurationError, NewConfigurationError};
use futures::{Stream, TryStreamExt as _};
use sqlx::sqlite::{SqliteConnectOptions, SqliteRow};
use sqlx::{query_as, FromRow, QueryBuilder, Row, Sqlite, SqlitePool};
use tracing::{info, instrument, trace};

use crate::paths::{
    BeamlineField, DetectorField, DetectorTemplate, InvalidPathTemplate, PathSpec, ScanField,
    ScanTemplate, VisitTemplate,
};
use crate::template::PathTemplate;

type SqliteTemplateResult<F> = Result<PathTemplate<F>, InvalidPathTemplate>;

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
struct RawPathTemplate<F>(String, PhantomData<F>);

impl<Spec> RawPathTemplate<Spec>
where
    Spec: PathSpec,
{
    fn as_template(&self) -> SqliteTemplateResult<Spec::Field> {
        Ok(Spec::new_checked(&self.0)?)
    }
}

impl<F> From<String> for RawPathTemplate<F> {
    fn from(value: String) -> Self {
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

impl<'r> FromRow<'r, SqliteRow> for BeamlineConfiguration {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(DbBeamlineConfig {
            id: None,
            name: row.try_get("name")?,
            scan_number: row.try_get("scan_number")?,
            visit: row.try_get::<String, _>("visit")?.into(),
            scan: row.try_get::<String, _>("scan")?.into(),
            detector: row.try_get::<String, _>("detector")?.into(),
            fallback_extension: row.try_get::<Option<String>, _>("fallback_extension")?,
            fallback_directory: row.try_get::<Option<String>, _>("fallback_directory")?,
        }
        .into())
    }
}

#[derive(Debug)]
pub struct BeamlineConfigurationUpdate {
    pub name: String,
    pub scan_number: Option<u32>,
    pub visit: Option<PathTemplate<BeamlineField>>,
    pub scan: Option<PathTemplate<ScanField>>,
    pub detector: Option<PathTemplate<DetectorField>>,
    pub directory: Option<String>,
    pub extension: Option<String>,
}

impl BeamlineConfigurationUpdate {
    // May panic if the update is empty - check first
    pub async fn update_beamline(
        &self,
        db: &SqliteScanPathService,
    ) -> Result<Option<BeamlineConfiguration>, sqlx::Error> {
        let mut q: QueryBuilder<Sqlite> = QueryBuilder::new("UPDATE beamline SET ");
        let mut fields = q.separated(", ");
        if let Some(num) = self.scan_number {
            fields.push("scan_number=");
            fields.push_bind_unseparated(num);
        }
        if let Some(visit) = &self.visit {
            fields.push("visit=");
            fields.push_bind_unseparated(visit.to_string());
        }
        if let Some(scan) = &self.scan {
            fields.push("scan=");
            fields.push_bind_unseparated(scan.to_string());
        }
        if let Some(detector) = &self.detector {
            fields.push("detector=");
            fields.push_bind_unseparated(detector.to_string());
        }
        if let Some(dir) = &self.directory {
            fields.push("fallback_directory=");
            fields.push_bind_unseparated(dir);
        }
        if let Some(ext) = &self.extension {
            if ext != &self.name {
                // extension defaults to beamline name
                fields.push("fallback_directory=");
                fields.push_bind_unseparated(ext);
            }
        }
        q.push(" WHERE name = ");
        q.push_bind(&self.name);
        q.push(" RETURNING *");

        trace!(
            beamline = self.name,
            query = q.sql(),
            "Updating beamline configuration",
        );

        q.build_query_as().fetch_optional(&db.pool).await
    }
    pub async fn insert_new(
        self,
        db: &SqliteScanPathService,
    ) -> Result<BeamlineConfiguration, NewConfigurationError> {
        let dbc = DbBeamlineConfig {
            id: None,
            name: self.name,
            scan_number: self.scan_number.unwrap_or(0) as i64,
            visit: self.visit.ok_or("visit")?.to_string(),
            scan: self.scan.ok_or("scan")?.to_string(),
            detector: self.detector.ok_or("detector")?.to_string(),
            fallback_directory: self.directory,
            fallback_extension: self.extension,
        };
        Ok(dbc.insert_into(db).await?)
    }
}

#[derive(Debug)]
struct DbBeamlineConfig {
    #[allow(unused)] // unused but allows use of 'SELECT * ...' queries
    id: Option<i64>,
    name: String,
    scan_number: i64,
    visit: String,
    scan: String,
    detector: String,
    fallback_directory: Option<String>,
    fallback_extension: Option<String>,
}

impl DbBeamlineConfig {
    pub async fn insert_into(
        self,
        db: &SqliteScanPathService,
    ) -> sqlx::Result<BeamlineConfiguration> {
        let bc = query_as!(
            DbBeamlineConfig,
            "INSERT INTO beamline
                (name, scan_number, visit, scan, detector, fallback_directory, fallback_extension)
            VALUES
                (?,?,?,?,?,?,?)
            RETURNING *",
            self.name,
            self.scan_number,
            self.visit,
            self.scan,
            self.detector,
            self.fallback_directory,
            self.fallback_extension
        )
        .fetch_one(&db.pool)
        .await?;
        Ok(bc.into())
    }
}

impl From<DbBeamlineConfig> for BeamlineConfiguration {
    fn from(value: DbBeamlineConfig) -> Self {
        let fallback = match (value.fallback_directory, value.fallback_extension) {
            (None, None) => None,
            (None, Some(_)) => None,
            (Some(dir), None) => Some(NumtrackerConfig {
                directory: dir,
                extension: value.name.clone(),
            }),
            (Some(dir), Some(ext)) => Some(NumtrackerConfig {
                directory: dir,
                extension: ext,
            }),
        };
        Self {
            name: value.name,
            scan_number: value.scan_number as u32,
            visit: value.visit.into(),
            scan: value.scan.into(),
            detector: value.detector.into(),
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

    #[derive(Debug)]
    pub enum NewConfigurationError {
        MissingField(String),
        Db(sqlx::Error),
    }
    impl Display for NewConfigurationError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                NewConfigurationError::MissingField(name) => {
                    write!(f, "Missing field {name:?} for new configuration")
                }
                NewConfigurationError::Db(e) => write!(f, "Error inserting new configuration: {e}"),
            }
        }
    }
    impl From<&str> for NewConfigurationError {
        fn from(value: &str) -> Self {
            Self::MissingField(value.into())
        }
    }
    impl From<sqlx::Error> for NewConfigurationError {
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
