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

#[derive(Debug, PartialEq, Eq)]
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
        Spec::new_checked(&self.0)
    }
}

impl<F> From<String> for RawPathTemplate<F> {
    fn from(value: String) -> Self {
        Self(value, PhantomData)
    }
}

#[derive(Debug)]
pub struct BeamlineConfiguration {
    name: String,
    scan_number: u32,
    visit: RawPathTemplate<VisitTemplate>,
    scan: RawPathTemplate<ScanTemplate>,
    detector: RawPathTemplate<DetectorTemplate>,
    extension: Option<String>,
}

impl BeamlineConfiguration {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scan_number(&self) -> u32 {
        self.scan_number
    }

    pub fn extension(&self) -> Option<&str> {
        self.extension.as_deref()
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
            visit: row.try_get::<String, _>("visit")?,
            scan: row.try_get::<String, _>("scan")?,
            detector: row.try_get::<String, _>("detector")?,
            fallback_extension: row.try_get::<Option<String>, _>("fallback_extension")?,
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
    pub extension: Option<String>,
}

impl BeamlineConfigurationUpdate {
    fn is_empty(&self) -> bool {
        self.scan_number.is_none()
            && self.visit.is_none()
            && self.scan.is_none()
            && self.detector.is_none()
            && self.extension.is_none()
    }

    pub async fn update_beamline(
        &self,
        db: &SqliteScanPathService,
    ) -> Result<Option<BeamlineConfiguration>, sqlx::Error> {
        if self.is_empty() {
            return match db.current_configuration(&self.name).await {
                Ok(bc) => Ok(Some(bc)),
                Err(ConfigurationError::MissingBeamline(_)) => Ok(None),
                Err(ConfigurationError::Db(e)) => Err(e),
            };
        }
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
        if let Some(ext) = &self.extension {
            if ext != &self.name {
                // extension defaults to beamline name
                fields.push("fallback_extension=");
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
            scan_number: i64::from(self.scan_number.unwrap_or(0)),
            visit: self.visit.ok_or("visit")?.to_string(),
            scan: self.scan.ok_or("scan")?.to_string(),
            detector: self.detector.ok_or("detector")?.to_string(),
            fallback_extension: self.extension,
        };
        Ok(dbc.insert_into(db).await?)
    }
    #[cfg(test)]
    fn empty(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            scan_number: None,
            visit: None,
            scan: None,
            detector: None,
            extension: None,
        }
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
                (name, scan_number, visit, scan, detector, fallback_extension)
            VALUES
                (?,?,?,?,?,?)
            RETURNING *",
            self.name,
            self.scan_number,
            self.visit,
            self.scan,
            self.detector,
            self.fallback_extension
        )
        .fetch_one(&db.pool)
        .await?;
        Ok(bc.into())
    }
}

impl From<DbBeamlineConfig> for BeamlineConfiguration {
    fn from(value: DbBeamlineConfig) -> Self {
        Self {
            name: value.name,
            scan_number: u32::try_from(value.scan_number).expect("Out of scan numbers"),
            visit: value.visit.into(),
            scan: value.scan.into(),
            detector: value.detector.into(),
            extension: value.fallback_extension,
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

    pub async fn current_configuration(
        &self,
        beamline: &str,
    ) -> Result<BeamlineConfiguration, ConfigurationError> {
        query_as!(
            DbBeamlineConfig,
            "SELECT * FROM beamline WHERE name = ?",
            beamline
        )
        .fetch_optional(&self.pool)
        .await?
        .map(BeamlineConfiguration::from)
        .ok_or(ConfigurationError::MissingBeamline(beamline.into()))
    }

    pub async fn configurations(&self) -> Result<Vec<BeamlineConfiguration>, ConfigurationError> {
        Ok(query_as!(DbBeamlineConfig, "SELECT * FROM beamline")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(BeamlineConfiguration::from)
            .collect())
    }

    pub async fn next_scan_configuration(
        &self,
        beamline: &str,
        current_high: Option<u32>,
    ) -> Result<BeamlineConfiguration, ConfigurationError> {
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
        .ok_or(ConfigurationError::MissingBeamline(beamline.into()))
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
    pub enum ConfigurationError {
        MissingBeamline(String),
        Db(sqlx::Error),
    }

    impl Display for ConfigurationError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                ConfigurationError::MissingBeamline(bl) => {
                    write!(f, "No configuration available for beamline {bl:?}")
                }
                ConfigurationError::Db(e) => write!(f, "Error reading configuration: {e}"),
            }
        }
    }

    impl Error for ConfigurationError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                ConfigurationError::MissingBeamline(_) => None,
                ConfigurationError::Db(e) => Some(e),
            }
        }
    }

    impl From<sqlx::Error> for ConfigurationError {
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
    use sqlx::error::{DatabaseError as _, ErrorKind};
    use sqlx::sqlite::SqliteError;
    use tokio::test;

    use super::SqliteScanPathService;
    use crate::db_service::error::{ConfigurationError, NewConfigurationError};
    use crate::db_service::{BeamlineConfiguration, BeamlineConfigurationUpdate};
    use crate::paths::{DetectorTemplate, PathSpec, ScanTemplate, VisitTemplate};

    /// Remove repeated .await.unwrap() noise from tests
    macro_rules! ok {
        ($call:expr) => {
            $call.await.unwrap()
        };
    }
    /// Remove repeated .await.unwrap_err() noise from tests
    /// Await the given future, unwrap the err and match it against the expected pattern
    macro_rules! err {
        ($call:expr) => {
            $call.await.unwrap_err()
        };
        ($exp:path, $call:expr) => {{
            let e = $call.await.unwrap_err();
            let $exp(e) = e else {
                panic!("Unexpected error: {e}");
            };
            e
        }};
    }

    #[fixture]
    async fn db() -> SqliteScanPathService {
        let db = SqliteScanPathService::memory().await;
        update().insert_new(&db).await.unwrap();
        db
    }

    #[fixture]
    fn update() -> BeamlineConfigurationUpdate {
        BeamlineConfigurationUpdate {
            name: "i22".into(),
            scan_number: Some(122),
            visit: VisitTemplate::new_checked("/tmp/{instrument}/data/{year}/{visit}").ok(),
            scan: ScanTemplate::new_checked("{subdirectory}/{instrument}-{scan_number}").ok(),
            detector: DetectorTemplate::new_checked(
                "{subdirectory}/{instrument}-{scan_number}-{detector}",
            )
            .ok(),
            extension: Some("ext".into()),
        }
    }

    #[test]
    async fn empty_db_has_no_config() {
        let db = SqliteScanPathService::memory().await;
        let e = err!(
            ConfigurationError::MissingBeamline,
            db.current_configuration("i22")
        );
        assert_eq!(e, "i22")
    }

    #[rstest]
    #[case::visit("visit", |u: &mut BeamlineConfigurationUpdate| u.visit = None)]
    #[case::scan("scan", |u: &mut BeamlineConfigurationUpdate| u.scan = None)]
    #[case::scan("detector", |u: &mut BeamlineConfigurationUpdate| u.detector = None)]
    #[tokio::test]
    async fn enew_beamline_with_missing_field(
        mut update: BeamlineConfigurationUpdate,
        #[case] name: &str,
        #[case] init: impl FnOnce(&mut BeamlineConfigurationUpdate),
    ) {
        let db = SqliteScanPathService::memory().await;
        init(&mut update);
        let field = err!(NewConfigurationError::MissingField, update.insert_new(&db));
        assert_eq!(field, name);
    }

    #[rstest]
    #[case::directory(|u: &mut BeamlineConfigurationUpdate| u.extension = None)]
    #[case::scan_number(|u: &mut BeamlineConfigurationUpdate| u.scan_number = None)]
    #[tokio::test]
    async fn new_beamline_without_optional(
        mut update: BeamlineConfigurationUpdate,
        #[case] init: impl FnOnce(&mut BeamlineConfigurationUpdate),
    ) {
        let db = SqliteScanPathService::memory().await;
        init(&mut update);
        let bc = ok!(update.insert_new(&db));
        assert_eq!(bc.name(), "i22");
    }

    #[rstest]
    #[test]
    async fn read_only_db_propagates_errors(update: BeamlineConfigurationUpdate) {
        let db = SqliteScanPathService::ro_memory().await;
        let e = err!(NewConfigurationError::Db, update.insert_new(&db));
        let e = e.into_database_error().unwrap().downcast::<SqliteError>();
        assert_eq!(e.kind(), ErrorKind::Other);
    }

    #[rstest]
    #[test]
    async fn read_only_db_propagates_error(update: BeamlineConfigurationUpdate) {
        let db = SqliteScanPathService::ro_memory().await;
        let e = err!(NewConfigurationError::Db, update.insert_new(&db));
        let e = e.into_database_error().unwrap().downcast::<SqliteError>();
        assert_eq!(e.kind(), ErrorKind::Other);
    }

    #[test]
    async fn duplicate_beamlines() {
        let db = SqliteScanPathService::memory().await;
        _ = ok!(update().insert_new(&db));
        let e = err!(NewConfigurationError::Db, update().insert_new(&db));
        let e = e.into_database_error().unwrap().downcast::<SqliteError>();
        assert_eq!(e.kind(), ErrorKind::UniqueViolation);
    }

    #[rstest]
    #[test]
    async fn incrementing_scan_numbers(#[future(awt)] db: SqliteScanPathService) {
        let s1 = ok!(db.next_scan_configuration("i22", None));
        let s2 = ok!(db.next_scan_configuration("i22", None));
        assert_eq!(s1.scan_number() + 1, s2.scan_number());
    }

    #[rstest]
    #[test]
    async fn overriding_scan_number_updates_db(#[future(awt)] db: SqliteScanPathService) {
        let s1 = ok!(db.next_scan_configuration("i22", None));
        let s2 = ok!(db.next_scan_configuration("i22", Some(1234)));
        let s3 = ok!(db.next_scan_configuration("i22", None));
        assert_eq!(s1.scan_number(), 123);
        assert_eq!(s2.scan_number(), 1235);
        assert_eq!(s3.scan_number(), 1236);
    }

    #[rstest]
    #[test]
    async fn lower_scan_override_is_ignored(#[future(awt)] db: SqliteScanPathService) {
        let s1 = ok!(db.next_scan_configuration("i22", Some(42)));
        assert_eq!(s1.scan_number(), 123);
    }

    #[rstest]
    #[test]
    async fn incrementing_missing_beamline(#[future(awt)] db: SqliteScanPathService) {
        let e = err!(
            ConfigurationError::MissingBeamline,
            db.next_scan_configuration("b21", None)
        );
        assert_eq!(e, "b21")
    }

    #[rstest]
    #[test]
    async fn current_configuration(#[future(awt)] db: SqliteScanPathService) {
        let conf = ok!(db.current_configuration("i22"));
        assert_eq!(conf.name(), "i22");
        assert_eq!(conf.scan_number(), 122);
        assert_eq!(
            conf.visit().unwrap().to_string(),
            "/tmp/{instrument}/data/{year}/{visit}"
        );
        assert_eq!(
            conf.scan().unwrap().to_string(),
            "{subdirectory}/{instrument}-{scan_number}"
        );
        assert_eq!(
            conf.detector().unwrap().to_string(),
            "{subdirectory}/{instrument}-{scan_number}-{detector}"
        );
        let Some(ext) = conf.extension() else {
            panic!("Missing extension");
        };
        assert_eq!(ext, "ext");
    }

    #[rstest]
    #[test]
    async fn configurations(#[future(awt)] db: SqliteScanPathService) {
        let confs = ok!(db.configurations());
        assert_eq!(confs.len(), 1);
        let conf = confs.first().unwrap();
        assert_eq!(conf.name(), "i22");
        assert_eq!(conf.scan_number(), 122);
        assert_eq!(
            conf.visit().unwrap().to_string(),
            "/tmp/{instrument}/data/{year}/{visit}"
        );
        assert_eq!(
            conf.scan().unwrap().to_string(),
            "{subdirectory}/{instrument}-{scan_number}"
        );
        assert_eq!(
            conf.detector().unwrap().to_string(),
            "{subdirectory}/{instrument}-{scan_number}-{detector}"
        );
        let Some(fb) = conf.fallback() else {
            panic!("Missing fallback configuration");
        };
        assert_eq!(fb.directory, "/tmp/trackers");
        assert_eq!(fb.extension, "ext");
    }

    type Update = BeamlineConfigurationUpdate;

    #[rstest]
    #[case::visit(
            |u: &mut Update| u.visit = VisitTemplate::new_checked("/new/{instrument}/{proposal}/{visit}").ok(),
            |u: BeamlineConfiguration| assert_eq!(u.visit().unwrap().to_string(), "/new/{instrument}/{proposal}/{visit}"))]
    #[case::scan(
            |u: &mut Update| u.scan = ScanTemplate::new_checked("new-{scan_number}").ok(),
            |u: BeamlineConfiguration| assert_eq!(u.scan().unwrap().to_string(), "new-{scan_number}"))]
    #[case::detector(
            |u: &mut Update| u.detector = DetectorTemplate::new_checked("new-{scan_number}-{detector}").ok(),
            |u: BeamlineConfiguration| assert_eq!(u.detector().unwrap().to_string(), "new-{scan_number}-{detector}"))]
    #[case::scan_number(
            |u: &mut Update| u.scan_number = Some(42),
            |u: BeamlineConfiguration| assert_eq!(u.scan_number(), 42))]
    #[case::extension(
            |u: &mut Update| u.extension = Some("new".into()),
            |u: BeamlineConfiguration| assert_eq!(u.extension().unwrap(), "new"))]
    #[tokio::test]
    async fn update_existing(
        #[future(awt)] db: SqliteScanPathService,
        #[case] init: impl FnOnce(&mut Update),
        #[case] check: impl FnOnce(BeamlineConfiguration),
    ) {
        let mut upd = Update::empty("i22");
        init(&mut upd);
        let bc = ok!(upd.update_beamline(&db)).expect("Updated beamline missing");
        check(bc)
    }

    #[rstest]
    #[tokio::test]
    async fn empty_update(#[future(awt)] db: SqliteScanPathService) {
        let upd = BeamlineConfigurationUpdate::empty("b21");
        assert!(ok!(upd.update_beamline(&db)).is_none());
    }
}
