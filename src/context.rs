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

use std::borrow::Cow;
use std::error::Error;
use std::fmt::Display;
use std::path::{Component, Path, PathBuf};

use chrono::{Datelike as _, Local};
use tracing::{debug, info, instrument};

use crate::db_service::{SqliteNumberError, SqliteScanPathService, SqliteTemplateError};
use crate::paths::{BeamlineField, DetectorField, ScanField};
use crate::template::FieldSource;

pub struct VisitService {
    db: SqliteScanPathService,
    ctx: BeamlineContext,
}

pub struct ScanService {
    db: SqliteScanPathService,
    ctx: ScanContext,
}

#[derive(Clone)]
pub struct BeamlineContext {
    instrument: String,
    visit: String,
}

pub struct ScanContext {
    subdirectory: Subdirectory,
    scan_number: usize,
    beamline: BeamlineContext,
}

struct DetectorContext<'bl> {
    detector: Detector,
    scan: &'bl ScanContext,
}

impl<'bl> VisitService {
    pub fn new(backend: SqliteScanPathService, ctx: BeamlineContext) -> Self {
        Self { db: backend, ctx }
    }
    pub fn beamline(&self) -> &str {
        &self.ctx.instrument
    }
    pub fn visit(&self) -> &str {
        &self.ctx.visit
    }

    #[instrument(skip(self))]
    pub async fn new_scan(
        &self,
        subdirectory: Subdirectory,
    ) -> Result<ScanService, SqliteNumberError> {
        let number = self.db.next_scan_number(&self.ctx.instrument).await?;
        info!("Next scan number for {}: {number}", self.ctx.instrument);
        Ok(ScanService {
            db: self.db.clone(),
            ctx: self.ctx.for_scan(number, subdirectory),
        })
    }

    #[instrument(skip(self))]
    pub async fn visit_directory(&self) -> Result<PathBuf, SqliteTemplateError> {
        let template = self
            .db
            .visit_directory_template(&self.ctx.instrument)
            .await?;
        info!("Visit template: {template:?}");
        Ok(template.render(&self.ctx))
    }
}

impl ScanService {
    pub fn as_visit_service(&self) -> VisitService {
        VisitService {
            db: self.db.clone(),
            ctx: self.ctx.beamline.clone(),
        }
    }
    #[instrument(skip(self))]
    pub fn scan_number(&self) -> usize {
        self.ctx.scan_number
    }
    #[instrument(skip(self))]
    pub fn beamline(&self) -> &str {
        &self.ctx.beamline.instrument
    }
    #[instrument(skip(self))]
    pub fn visit(&self) -> &str {
        &self.ctx.beamline.visit
    }

    #[instrument(skip(self))]
    pub async fn visit_directory(&self) -> Result<PathBuf, SqliteTemplateError> {
        let template = self.db.visit_directory_template(self.beamline()).await?;
        info!("Visit template: {template:?}");
        Ok(template.render(&self.ctx.beamline))
    }

    #[instrument(skip(self))]
    pub async fn scan_file(&self) -> Result<PathBuf, SqliteTemplateError> {
        let scan_file_template = self
            .db
            .scan_file_template(&self.ctx.beamline.instrument)
            .await?;
        info!("Scan file template: {scan_file_template:?}");
        Ok(scan_file_template.render(&self.ctx))
    }

    #[instrument(skip(self), fields(detectors))]
    pub async fn detector_files<'det>(
        &self,
        detectors: &'det [String],
    ) -> Result<Vec<(&'det String, PathBuf)>, SqliteTemplateError> {
        if detectors.is_empty() {
            debug!("Detectors list is empty so skipping template lookup");
            return Ok(vec![]);
        }
        let template = self
            .db
            .detector_file_template(&self.ctx.beamline.instrument)
            .await?;
        info!(
            "Detector template for {}: {:?}",
            self.ctx.beamline.instrument, template
        );
        Ok(detectors
            .iter()
            .map(|det| {
                let path = template.render(&self.ctx.for_detector(det));
                (det, path)
            })
            .collect())
    }
}

impl FieldSource<BeamlineField> for BeamlineContext {
    fn resolve(&self, field: &BeamlineField) -> Cow<'_, str> {
        match field {
            // Should be year of visit?
            BeamlineField::Year => Local::now().year().to_string().into(),
            BeamlineField::Visit => self.visit().into(),
            BeamlineField::Proposal => self
                .visit
                .split('-')
                .next()
                .expect("There is always one section for a split")
                .into(),
            BeamlineField::Instrument => AsRef::<str>::as_ref(&self.instrument).into(),
        }
    }
}

impl FieldSource<ScanField> for ScanContext {
    fn resolve(&self, field: &ScanField) -> Cow<'_, str> {
        match field {
            ScanField::Subdirectory => self.subdirectory.as_ref().to_string_lossy(),
            ScanField::ScanNumber => self.scan_number.to_string().into(),
            ScanField::Beamline(bf) => self.beamline.resolve(bf),
        }
    }
}

impl<'a> FieldSource<DetectorField> for DetectorContext<'a> {
    fn resolve(&self, field: &DetectorField) -> Cow<'_, str> {
        match field {
            DetectorField::Detector => self.detector.as_ref().into(),
            DetectorField::Scan(sf) => self.scan.resolve(sf),
        }
    }
}

#[derive(Debug)]
pub struct Detector(String);
impl Detector {
    const INVALID: fn(char) -> bool = |c| !c.is_ascii_alphanumeric();
}

impl From<String> for Detector {
    fn from(value: String) -> Self {
        if value.contains(Self::INVALID) {
            value.as_str().into()
        } else {
            Self(value)
        }
    }
}

impl From<&str> for Detector {
    fn from(value: &str) -> Self {
        Self(value.replace(Self::INVALID, "_"))
    }
}

impl AsRef<str> for Detector {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

// Derived Default is OK without validation as empty path is a valid subdirectory
#[derive(Debug, Default)]
pub struct Subdirectory(PathBuf);

#[derive(Debug)]
pub enum InvalidSubdirectory {
    InvalidComponent(usize),
    AbsolutePath,
}

impl Display for InvalidSubdirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidSubdirectory::InvalidComponent(s) => {
                write!(f, "Segment {s} of path is not valid for a subdirectory")
            }
            InvalidSubdirectory::AbsolutePath => f.write_str("Subdirectory cannot be absolute"),
        }
    }
}

impl Error for InvalidSubdirectory {}

impl Subdirectory {
    pub fn new(sub: impl Into<PathBuf>) -> Result<Self, InvalidSubdirectory> {
        let sub = sub.into();
        let mut new_sub = PathBuf::new();
        for (i, comp) in sub.components().enumerate() {
            let err = match comp {
                Component::CurDir => continue,
                Component::Normal(seg) => {
                    new_sub.push(seg);
                    continue;
                }
                Component::RootDir => InvalidSubdirectory::AbsolutePath,
                Component::Prefix(_) | Component::ParentDir => {
                    InvalidSubdirectory::InvalidComponent(i)
                }
            };
            return Err(err);
        }
        Ok(Self(new_sub))
    }
}

impl Display for Subdirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.display().fmt(f)
    }
}

impl AsRef<Path> for Subdirectory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl BeamlineContext {
    pub fn new(instrument: String, visit: String) -> Self {
        Self { instrument, visit }
    }
    pub fn instrument(&self) -> &str {
        &self.instrument
    }
    pub fn visit(&self) -> &str {
        &self.visit
    }
    pub fn for_scan(&self, scan_number: usize, subdirectory: Subdirectory) -> ScanContext {
        ScanContext {
            subdirectory,
            scan_number,
            beamline: self.clone(),
        }
    }
}

impl ScanContext {
    pub fn with_subdirectory(self, sub: Subdirectory) -> Self {
        Self {
            subdirectory: sub,
            ..self
        }
    }

    fn for_detector(&self, det: &str) -> DetectorContext {
        DetectorContext {
            scan: self,
            detector: det.into(),
        }
    }
}

#[cfg(test)]
mod detector_tests {
    use super::Detector;

    #[test]
    fn valid() {
        assert_eq!("valid_detector", Detector::from("valid_detector").as_ref());
    }

    #[test]
    fn invalid() {
        assert_eq!(
            Detector::from("spaced detector").as_ref(),
            "spaced_detector",
        );
        assert_eq!(Detector::from("..").as_ref(), "__");
        assert_eq!(Detector::from("foo.bar").as_ref(), "foo_bar");
        assert_eq!(Detector::from("foo/bar").as_ref(), "foo_bar");
    }
}
