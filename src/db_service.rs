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

use std::fmt::{self, Display};
use std::path::Path;

use error::UpdateSingleError;
use futures::Stream;
use sqlx::prelude::FromRow;
use sqlx::query::{Query, QueryScalar, QueryScalar};
use sqlx::sqlite::{SqliteArguments, SqliteConnectOptions, SqliteConnectOptions};
use sqlx::{query_as, query_scalar};
use tracing::{debug, info, instrument, warn};

pub use self::error::{
    InsertTemplateError, SqliteNumberDirectoryError, SqliteNumberError, SqliteTemplateError,
};
use crate::cli::TemplateKind;
use crate::numtracker;
use crate::paths::{BeamlineField, DetectorField, InvalidKey, ScanField};
use crate::template::PathTemplate;

type SqliteTemplateResult<F> = Result<PathTemplate<F>, SqliteTemplateError>;

#[derive(Clone)]
pub struct SqliteScanPathService {
    pool: SqlitePool,
}

#[derive(Debug, FromRow)]
pub struct NumtrackerConfig {
    pub directory: String,
    pub extension: String,
}

#[derive(Debug)]
pub struct TemplateOption {
    pub id: i64,
    pub template: String,
}

impl Display for TemplateOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.template.fmt(f)
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

    /// Execute a prepared query and parse the returned string into a [`PathTemplate`]
    async fn template_from<'bl, F: TryFrom<String, Error = InvalidKey>>(
        &self,
        query: QueryScalar<'bl, Sqlite, String, SqliteArguments<'bl>>,
    ) -> SqliteTemplateResult<F> {
        let template = query
            .fetch_optional(&self.pool)
            .await?
            .ok_or(SqliteTemplateError::BeamlineNotFound)?;
        debug!(template = template, "Template from DB");

        Ok(PathTemplate::new(template)?)
    }

    /// Insert a new record into the database, or return the ID if the record is already
    /// present
    async fn get_or_insert<'q>(
        &self,
        insert: QueryScalar<'q, Sqlite, i64, SqliteArguments<'q>>,
        get: impl FnOnce() -> QueryScalar<'q, Sqlite, Option<i64>, SqliteArguments<'q>>,
    ) -> sqlx::Result<i64> {
        let mut trn = self.pool.begin().await?;
        let inserted = insert.fetch_optional(&mut *trn).await?;
        match inserted {
            Some(ins) => {
                trn.commit().await?;
                Ok(ins)
            }
            None => Ok(get()
                .fetch_one(&mut *trn)
                .await?
                // None here suggests an error in the queries being passed
                .expect("Record missing after being inserted")),
        }
    }

    async fn update_single<'q>(
        &self,
        query: Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> Result<(), UpdateSingleError> {
        let mut trn = self.pool.begin().await?;
        let res = query.execute(&mut *trn).await?;
        match res.rows_affected() {
            0 => Err(UpdateSingleError::ZeroRecords),
            2.. => Err(UpdateSingleError::TooMany),
            _ => {
                trn.commit().await?;
                Ok(())
            }
        }
    }

    pub async fn insert_beamline(&self, beamline: &str) -> Result<i64, SqliteNumberError> {
        Ok(self
            .get_or_insert(
                query_scalar!(
                    "INSERT INTO beamline (name) VALUES (?) ON CONFLICT DO NOTHING RETURNING id;",
                    beamline
                ),
                || query_scalar!("SELECT (id) FROM beamline WHERE name = ?", beamline),
            )
            .await?)
    }

    pub async fn next_scan_number(&self, beamline: &str) -> Result<usize, SqliteNumberError> {
        let next = self.db_scan_number(beamline).await?;
        let fallback = self.directory_scan_number(beamline).await;
        match fallback {
            Ok(n) if n != next => {
                warn!("Fallback numbering inconsistent. Expected: {next}, found {n}")
            }
            Err(e) => warn!("Error incrementing fallback directory number: {e}"),
            Ok(_) => {}
        }
        Ok(next)
    }

    pub async fn latest_scan_number(&self, beamline: &str) -> Result<usize, SqliteNumberError> {
        let number = query_file_scalar!("queries/latest_scan_number.sql", beamline)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(SqliteNumberError::BeamlineNotFound)?;

        number
            .try_into()
            .map_err(|_| SqliteNumberError::InvalidValue(number))
    }

    /// Increment and return the latest scan number for the given beamline
    async fn db_scan_number(&self, beamline: &str) -> Result<usize, SqliteNumberError> {
        let mut db = self.pool.begin().await?;
        let next = query_file_scalar!("queries/increment_scan_number.sql", beamline)
            .fetch_optional(&mut *db)
            .await?
            .ok_or(SqliteNumberError::BeamlineNotFound)?;
        let next = next
            .try_into()
            .map_err(|_| SqliteNumberError::InvalidValue(next))?;
        debug!("Next scan number: {next}");
        db.commit().await?;
        Ok(next)
    }
    async fn directory_scan_number(
        &self,
        beamline: &str,
    ) -> Result<usize, SqliteNumberDirectoryError> {
        match self.number_tracker_directory(beamline).await? {
            Some(nc) => Ok(numtracker::increment_and_get(&nc.directory, &nc.extension).await?),
            None => Err(SqliteNumberDirectoryError::NotConfigured),
        }
    }
    #[instrument]
    pub async fn visit_directory_template(
        &self,
        beamline: &str,
    ) -> SqliteTemplateResult<BeamlineField> {
        self.template_from(query_file_scalar!("queries/visit_template.sql", beamline))
            .await
    }
    #[instrument]
    pub async fn scan_file_template(&self, beamline: &str) -> SqliteTemplateResult<ScanField> {
        self.template_from(query_file_scalar!("queries/scan_template.sql", beamline))
            .await
    }
    #[instrument]
    pub async fn detector_file_template(
        &self,
        beamline: &str,
    ) -> SqliteTemplateResult<DetectorField> {
        self.template_from(query_file_scalar!(
            "queries/detector_template.sql",
            beamline
        ))
        .await
    }

    pub fn beamlines(&self) -> impl Stream<Item = Result<String, sqlx::Error>> + '_ {
        query_file_scalar!("queries/all_beamlines.sql").fetch(&self.pool)
    }

    pub async fn number_tracker_directory(
        &self,
        beamline: &str,
    ) -> Result<Option<NumtrackerConfig>, sqlx::Error> {
        debug!("Getting number_tracker_directory for {beamline}");
        query_file_as!(
            NumtrackerConfig,
            "queries/number_file_directory.sql",
            beamline
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn set_scan_number(&self, bl: &str, number: usize) -> Result<(), SqliteNumberError> {
        let number = number as i64;
        debug!(
            beamline = bl,
            scan_number = number,
            "Setting scan number directly"
        );
        match self
            .update_single(query_file!("queries/set_scan_number.sql", number, bl))
            .await
        {
            Ok(_) => Ok(()),
            Err(UpdateSingleError::Db(e)) => Err(e.into()),
            Err(UpdateSingleError::ZeroRecords) => Err(SqliteNumberError::BeamlineNotFound),
            Err(UpdateSingleError::TooMany) => {
                // This should never happen if the schema constraints are set correctly
                Err(SqliteNumberError::DuplicateBeamlineError)
            }
        }
    }

    async fn update_single<'q>(
        &self,
        query: Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> Result<(), sqlx::Error> {
        let mut trn = self.pool.begin().await?;
        let res = query.execute(&mut *trn).await?;
        match res.rows_affected() {
            0 => panic!("No entries updated"),
            n @ 2.. => panic!("Two many entries updated ({n})"),
            _ => trn.commit().await?,
        }
        // if res.rows_affected() != 1 {
        //     trn.rollback().await?;
        //     panic!(
        //         "Incorrect number of entries updated: {}",
        //         res.rows_affected()
        //     );
        // } else {
        //     trn.commit().await?;
        // }
        Ok(())
    }

    pub async fn set_beamline_template(
        &self,
        bl: &str,
        kind: TemplateKind,
        template_id: i64,
    ) -> sqlx::Result<()> {
        debug!(
            beamline = bl,
            template_id, "Setting beamline {kind:?} template"
        );
        match kind {
            TemplateKind::Visit => {
                self.update_single(query_file!(
                    "queries/set_visit_template.sql",
                    template_id,
                    bl
                ))
                .await
            }
            TemplateKind::Scan => {
                self.update_single(query_file!(
                    "queries/set_scan_template.sql",
                    template_id,
                    bl
                ))
                .await
            }
            TemplateKind::Detector => {
                self.update_single(query_file!(
                    "queries/set_detector_template.sql",
                    template_id,
                    bl
                ))
                .await
            }
        }
    }

    pub async fn insert_beamline(&self, beamline: &str) -> Result<i64, sqlx::Error> {
        self.get_or_insert(
            query_file_scalar!("queries/insert_beamline.sql", beamline),
            || query_file_scalar!("queries/get_beamline_id.sql", beamline),
        )
        .await
    }

    pub async fn insert_template(
        &self,
        kind: TemplateKind,
        template: String,
    ) -> Result<i64, InsertTemplateError> {
        kind.validate(&template)?;
        let template = template.as_str();
        let new_id = match kind {
            TemplateKind::Visit => {
                self.get_or_insert(
                    query_file_scalar!("queries/insert_visit_template.sql", template),
                    || query_file_scalar!("queries/get_visit_template.sql", template),
                )
                .await
            }
            TemplateKind::Scan => {
                self.get_or_insert(
                    query_file_scalar!("queries/insert_scan_template.sql", template),
                    || query_file_scalar!("queries/get_scan_template.sql", template),
                )
                .await
            }
            TemplateKind::Detector => {
                self.get_or_insert(
                    query_file_scalar!("queries/insert_detector_template.sql", template),
                    || query_file_scalar!("queries/get_detector_template.sql", template),
                )
                .await
            }
        }?;
        Ok(new_id)
    }

    pub async fn get_templates(&self, kind: TemplateKind) -> sqlx::Result<Vec<TemplateOption>> {
        match kind {
            TemplateKind::Visit => {
                query_as!(TemplateOption, "SELECT id, template FROM visit_template;")
                    .fetch_all(&self.pool)
                    .await
            }
            TemplateKind::Scan => {
                query_as!(TemplateOption, "SELECT id, template FROM scan_template;")
                    .fetch_all(&self.pool)
                    .await
            }
            TemplateKind::Detector => {
                query_as!(
                    TemplateOption,
                    "SELECT id, template FROM detector_template;"
                )
                .fetch_all(&self.pool)
                .await
            }
        }
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
    use crate::template::PathTemplateError;

    /// Something that went wrong in the chain of querying the database for a template and
    /// converting it into a usable template.
    #[derive(Debug)]
    pub enum SqliteTemplateError {
        /// It wasn't possible to get the requested template from the database.
        ConnectionError(sqlx::Error),
        /// There is no template available for the requested beamline
        BeamlineNotFound,
        /// The template was present in the database but it could not be parsed into a valid
        /// [`PathTemplate`].
        Invalid(PathTemplateError),
    }

    impl Display for SqliteTemplateError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::ConnectionError(e) => write!(f, "Could not access database: {e}"),
                Self::BeamlineNotFound => f.write_str("No template found for beamline"),
                Self::Invalid(e) => write!(f, "Template is not valid: {e}"),
            }
        }
    }

    impl Error for SqliteTemplateError {}

    impl From<sqlx::Error> for SqliteTemplateError {
        fn from(sql: sqlx::Error) -> Self {
            Self::ConnectionError(sql)
        }
    }

    impl From<PathTemplateError> for SqliteTemplateError {
        fn from(err: PathTemplateError) -> Self {
            Self::Invalid(err)
        }
    }

    #[derive(Debug)]
    pub enum SqliteNumberError {
        BeamlineNotFound,
        DuplicateBeamlineError,
        ConnectionError(sqlx::Error),
        InvalidValue(i64),
    }

    impl Display for SqliteNumberError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BeamlineNotFound => f.write_str("No scan number configured for beamline"),
                Self::ConnectionError(e) => write!(f, "Could not access DB: {e}"),
                Self::InvalidValue(v) => write!(f, "Scan number {v} in DB is not valid"),
                Self::DuplicateBeamlineError => {
                    write!(f, "Invalid DB state - multiple entries for beamline")
                }
            }
        }
    }

    impl Error for SqliteNumberError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                Self::BeamlineNotFound | Self::InvalidValue(_) | Self::DuplicateBeamlineError => {
                    None
                }
                Self::ConnectionError(e) => Some(e),
            }
        }
    }

    impl From<sqlx::Error> for SqliteNumberError {
        fn from(value: sqlx::Error) -> Self {
            Self::ConnectionError(value)
        }
    }

    #[derive(Debug)]
    pub enum SqliteNumberDirectoryError {
        /// There is no directory configured for the requested beamline
        NotConfigured,
        /// The DB could not be accessed or queried
        NotAccessible(sqlx::Error),
        /// The directory was not present or not readable
        NotReabable(std::io::Error),
    }

    impl Display for SqliteNumberDirectoryError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::NotConfigured => {
                    f.write_str("No directory configured for the given beamline")
                }
                Self::NotAccessible(e) => e.fmt(f),
                Self::NotReabable(e) => e.fmt(f),
            }
        }
    }

    impl Error for SqliteNumberDirectoryError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                Self::NotConfigured => None,
                Self::NotAccessible(e) => Some(e),
                Self::NotReabable(e) => Some(e),
            }
        }
    }

    impl From<std::io::Error> for SqliteNumberDirectoryError {
        fn from(value: std::io::Error) -> Self {
            Self::NotReabable(value)
        }
    }

    impl From<sqlx::Error> for SqliteNumberDirectoryError {
        fn from(value: sqlx::Error) -> Self {
            Self::NotAccessible(value)
        }
    }

    #[derive(Debug)]
    pub enum InsertTemplateError {
        Db(sqlx::Error),
        Invalid(InvalidPathTemplate),
    }

    impl Display for InsertTemplateError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                InsertTemplateError::Db(e) => write!(f, "Error inserting template: {e}"),
                InsertTemplateError::Invalid(e) => write!(f, "Template was not valid: {e}"),
            }
        }
    }

    impl Error for InsertTemplateError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                InsertTemplateError::Db(e) => Some(e),
                InsertTemplateError::Invalid(e) => Some(e),
            }
        }
    }

    impl From<InvalidPathTemplate> for InsertTemplateError {
        fn from(value: InvalidPathTemplate) -> Self {
            Self::Invalid(value)
        }
    }

    impl From<sqlx::Error> for InsertTemplateError {
        fn from(value: sqlx::Error) -> Self {
            Self::Db(value)
        }
    }

    pub enum UpdateSingleError {
        Db(sqlx::Error),
        ZeroRecords,
        TooMany,
    }

    impl From<sqlx::Error> for UpdateSingleError {
        fn from(value: sqlx::Error) -> Self {
            Self::Db(value)
        }
    }
}
