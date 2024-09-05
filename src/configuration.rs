#![deny(clippy::unwrap_used)]
use std::error::Error;
use std::fmt::Display;
use std::path::Path;

use inquire::Select;

use crate::cli::{BeamlineConfig, ConfigAction, TemplateAction, TemplateConfig, TemplateKind};
use crate::db_service::{InsertTemplateError, SqliteScanPathService, TemplateOption};
use crate::paths::InvalidPathTemplate;

pub async fn configure(db: &Path, opts: ConfigAction) -> Result<(), ConfigError> {
    let db = SqliteScanPathService::connect(db).await.unwrap();
    match opts {
        ConfigAction::Beamline(opts) => configure_beamline(&db, opts).await,
        ConfigAction::Template(opts) => configure_template(&db, opts).await,
    }
}

async fn configure_beamline(
    db: &SqliteScanPathService,
    opts: BeamlineConfig,
) -> Result<(), ConfigError> {
    println!("{opts:#?}");
    if let Some(visit) = opts.visit {
        let visit_id = set_template(&db, TemplateKind::Visit, visit).await?;
        db.set_visit_template(&opts.beamline, visit_id).await?;
    }
    if let Some(scan) = opts.scan {
        let scan_id = set_template(&db, TemplateKind::Scan, scan).await?;
        db.set_scan_template(&opts.beamline, scan_id).await?;
    }
    if let Some(detector) = opts.detector {
        let detector_id = set_template(&db, TemplateKind::Detector, detector).await?;
        db.set_detector_template(&opts.beamline, detector_id)
            .await?;
    }
    Ok(())
}

async fn configure_template(
    db: &SqliteScanPathService,
    opts: TemplateConfig,
) -> Result<(), ConfigError> {
    match opts.action {
        TemplateAction::Add { kind, template } => {
            println!("Adding {kind:?} template: {template:?}");
            db.insert_template(kind, template).await?;
        }
        TemplateAction::List { filter } => {
            if let Some(TemplateKind::Visit) | None = filter {
                list_templates("Visit", &db.get_templates(TemplateKind::Visit).await?)
            }
            if let Some(TemplateKind::Scan) | None = filter {
                list_templates("Scan", &db.get_templates(TemplateKind::Scan).await?)
            }
            if let Some(TemplateKind::Detector) | None = filter {
                list_templates("Detector", &db.get_templates(TemplateKind::Detector).await?)
            }
        }
    }
    Ok(())
}

fn list_templates(heading: &str, templates: &[TemplateOption]) {
    println!("{heading} Templates:");
    for tmp in templates {
        println!("    {}", tmp);
    }
}

async fn set_template(
    db: &SqliteScanPathService,
    kind: TemplateKind,
    template: Option<String>,
) -> Result<i64, ConfigError> {
    match template {
        Some(template) => Ok(new_template(db, kind, template).await?),
        None => choose_template(db, kind).await,
    }
}

async fn new_template(
    db: &SqliteScanPathService,
    kind: TemplateKind,
    template: String,
) -> Result<i64, ConfigError> {
    Ok(db.insert_template(kind, template).await?)
}

async fn choose_template(
    db: &SqliteScanPathService,
    kind: TemplateKind,
) -> Result<i64, ConfigError> {
    let templates = db.get_templates(kind).await?;

    Select::new(&format!("Choose a {kind:?} template: "), templates)
        .prompt()
        .map(|t| t.id)
        .map_err(|_| ConfigError::Cancelled)
}

#[derive(Debug)]
pub enum ConfigError {
    Cancelled,
    Db(sqlx::Error),
    InvalidTemplate(InvalidPathTemplate),
}

impl From<sqlx::Error> for ConfigError {
    fn from(value: sqlx::Error) -> Self {
        Self::Db(value)
    }
}

impl From<InsertTemplateError> for ConfigError {
    fn from(value: InsertTemplateError) -> Self {
        match value {
            InsertTemplateError::Db(e) => Self::Db(e),
            InsertTemplateError::Invalid(e) => Self::InvalidTemplate(e),
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Cancelled => write!(f, "User cancelled operation"),
            ConfigError::Db(db) => write!(f, "Error reading/writing to DB: {db}"),
            ConfigError::InvalidTemplate(e) => write!(f, "Template was not valid: {e}"),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConfigError::Cancelled => None,
            ConfigError::Db(db) => Some(db),
            ConfigError::InvalidTemplate(e) => Some(e),
        }
    }
}
