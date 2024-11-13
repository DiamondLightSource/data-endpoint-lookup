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

use std::collections::HashSet;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::hash::Hash;

use crate::template::{PathTemplate, PathTemplateError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BeamlineField {
    Year,
    Visit,
    Proposal,
    Instrument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanField {
    Subdirectory,
    ScanNumber,
    Beamline(BeamlineField),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DetectorField {
    Detector,
    Scan(ScanField),
}

impl Display for BeamlineField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BeamlineField::Year => f.write_str("year"),
            BeamlineField::Visit => f.write_str("visit"),
            BeamlineField::Proposal => f.write_str("proposal"),
            BeamlineField::Instrument => f.write_str("instrument"),
        }
    }
}

impl Display for ScanField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanField::Subdirectory => f.write_str("subdirectory"),
            ScanField::ScanNumber => f.write_str("scan_number"),
            ScanField::Beamline(bl) => write!(f, "{bl}"),
        }
    }
}

impl Display for DetectorField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DetectorField::Detector => f.write_str("detector"),
            DetectorField::Scan(sc) => write!(f, "{sc}"),
        }
    }
}

#[derive(Debug)]
pub struct InvalidKey(String);

impl Display for InvalidKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unrecognised key: {}", self.0)
    }
}

impl Error for InvalidKey {}

impl TryFrom<String> for BeamlineField {
    type Error = InvalidKey;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "year" => Ok(BeamlineField::Year),
            "visit" => Ok(BeamlineField::Visit),
            "proposal" => Ok(BeamlineField::Proposal),
            "instrument" => Ok(BeamlineField::Instrument),
            _ => Err(InvalidKey(value)),
        }
    }
}

impl TryFrom<String> for ScanField {
    type Error = InvalidKey;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "scan_number" => Ok(ScanField::ScanNumber),
            "subdirectory" => Ok(ScanField::Subdirectory),
            _ => Ok(ScanField::Beamline(BeamlineField::try_from(value)?)),
        }
    }
}

impl TryFrom<String> for DetectorField {
    type Error = InvalidKey;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "detector" => Ok(DetectorField::Detector),
            _ => Ok(DetectorField::Scan(ScanField::try_from(value)?)),
        }
    }
}

#[allow(unused)] // not actually unused: see github.com/rust-lang/rust/issues/128839
pub trait PathField: TryFrom<String> + Eq + Hash + Display + 'static {}
impl<F> PathField for F where F: TryFrom<String> + Eq + Hash + Display + 'static {}

pub trait PathSpec {
    type Field: PathField;
    const REQUIRED: &'static [Self::Field];
    const ABSOLUTE: bool;

    fn new_checked(path: &str) -> Result<PathTemplate<Self::Field>, InvalidPathTemplate> {
        let template = PathTemplate::new(path)?;
        match (Self::ABSOLUTE, template.is_absolute()) {
            (true, false) => Err(InvalidPathTemplate::ShouldBeAbsolute),
            (false, true) => Err(InvalidPathTemplate::ShouldBeRelative),
            _ => Ok(()),
        }?;
        let fields = template.referenced_fields().collect::<HashSet<_>>();
        for f in Self::REQUIRED {
            if !fields.contains(f) {
                return Err(InvalidPathTemplate::MissingField(f.to_string()));
            }
        }
        Ok(template)
    }

    fn describe() -> &'static str;
}

#[derive(Debug, PartialEq)]
pub enum InvalidPathTemplate {
    TemplateError(PathTemplateError),
    ShouldBeAbsolute,
    ShouldBeRelative,
    MissingField(String),
}

impl Display for InvalidPathTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidPathTemplate::TemplateError(e) => write!(f, "{e}"),
            InvalidPathTemplate::ShouldBeAbsolute => f.write_str("Path should be absolute"),
            InvalidPathTemplate::ShouldBeRelative => f.write_str("Path should be relative"),
            InvalidPathTemplate::MissingField(fld) => {
                write!(f, "Template should reference missing field: {fld:?}")
            }
        }
    }
}

impl Error for InvalidPathTemplate {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InvalidPathTemplate::TemplateError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<PathTemplateError> for InvalidPathTemplate {
    fn from(value: PathTemplateError) -> Self {
        Self::TemplateError(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VisitTemplate;
#[derive(Debug, Clone, Copy)]
pub struct ScanTemplate;
#[derive(Debug, Clone, Copy)]
pub struct DetectorTemplate;

impl PathSpec for VisitTemplate {
    type Field = BeamlineField;

    const REQUIRED: &'static [Self::Field] = &[BeamlineField::Instrument, BeamlineField::Visit];

    const ABSOLUTE: bool = true;
    fn describe() -> &'static str {
        "A template describing the path to the visit directory for a beamline"
    }
}

impl PathSpec for ScanTemplate {
    type Field = ScanField;

    const REQUIRED: &'static [Self::Field] = &[ScanField::ScanNumber];

    const ABSOLUTE: bool = false;
    fn describe() -> &'static str {
        "A template describing the location within a visit directory where the root scan file should be written"
    }
}

impl PathSpec for DetectorTemplate {
    type Field = DetectorField;

    const REQUIRED: &'static [Self::Field] = &[
        DetectorField::Detector,
        DetectorField::Scan(ScanField::ScanNumber),
    ];

    const ABSOLUTE: bool = false;
    fn describe() -> &'static str {
        concat!(
            "A template describing the location within a visit directory where ",
            "the data for a given detector should be written",
            "\n\n",
            "It should contain placeholders for {detector} and {scan_number} ",
            "to ensure paths are unique between scans and for multiple ",
            "detectors."
        )
    }
}

#[cfg(test)]
mod paths_tests {
    use std::fmt::Debug;

    use super::{
        DetectorTemplate, InvalidPathTemplate, PathSpec as _, ScanTemplate, VisitTemplate,
    };
    use crate::template::{ErrorKind, PathTemplateError};

    #[derive(Debug)]
    enum TemplateErrorType {
        Incomplete,
        Nested,
        Empty,
        Unrecognised,
    }

    impl PartialEq<InvalidPathTemplate> for TemplateErrorType {
        fn eq(&self, other: &InvalidPathTemplate) -> bool {
            match other {
                InvalidPathTemplate::TemplateError(PathTemplateError::TemplateError(e)) => {
                    matches!(
                        (self, e.kind()),
                        (Self::Incomplete, ErrorKind::Incomplete)
                            | (Self::Nested, ErrorKind::Nested)
                            | (Self::Unrecognised, ErrorKind::Unrecognised)
                            | (Self::Empty, ErrorKind::Empty)
                    )
                }
                _ => false,
            }
        }
    }

    #[rstest::rstest]
    #[case::relative("relative/visit/path", InvalidPathTemplate::ShouldBeAbsolute)]
    #[case::missing_visit("/{instrument}/data", InvalidPathTemplate::MissingField("visit".into()))]
    #[case::missing_instrument("/data/{visit}", InvalidPathTemplate::MissingField("instrument".into()))]
    #[case::invalid_path_incomplete("/data/{unclosed", TemplateErrorType::Incomplete)]
    #[case::invalid_path_empty("/data/{}", TemplateErrorType::Empty)]
    #[case::invalid_path_nested("/data/{nes{ted}}", TemplateErrorType::Nested)]
    #[case::invalid_path_unrecognised("/data/{scan_number}", TemplateErrorType::Unrecognised)]
    fn invalid_visit<E: PartialEq<InvalidPathTemplate> + Debug>(
        #[case] template: &str,
        #[case] err: E,
    ) {
        let e = VisitTemplate::new_checked(template).unwrap_err();
        assert_eq!(err, e);
    }

    #[rstest::rstest]
    #[case::absolute("/absolute/scan/path", InvalidPathTemplate::ShouldBeRelative)]
    #[case::missing_scan_number("no_scan_number", InvalidPathTemplate::MissingField("scan_number".into()))]
    #[case::invalid_path_incomplete("data/{unclosed", TemplateErrorType::Incomplete)]
    #[case::invalid_path_empty("data/{}", TemplateErrorType::Empty)]
    #[case::invalid_path_nested("data/{nes{ted}}", TemplateErrorType::Nested)]
    #[case::invalid_path_unrecognised("data/{detector}", TemplateErrorType::Unrecognised)]
    fn invalid_scan<E: PartialEq<InvalidPathTemplate> + Debug>(
        #[case] template: &str,
        #[case] err: E,
    ) {
        let e = ScanTemplate::new_checked(template).unwrap_err();
        assert_eq!(err, e);
    }

    #[rstest::rstest]
    #[case::absolute("/absolute/detector/path", InvalidPathTemplate::ShouldBeRelative)]
    #[case::missing_detector("{scan_number}", InvalidPathTemplate::MissingField("detector".into()))]
    #[case::missing_scan_number("{detector}", InvalidPathTemplate::MissingField("scan_number".into()))]
    #[case::invalid_path_incomplete("data/{unclosed", TemplateErrorType::Incomplete)]
    #[case::invalid_path_empty("data/{}", TemplateErrorType::Empty)]
    #[case::invalid_path_nested("data/{nes{ted}}", TemplateErrorType::Nested)]
    #[case::invalid_path_unrecognised("data/{unknown}", TemplateErrorType::Unrecognised)]
    fn invalid_detector<E: PartialEq<InvalidPathTemplate> + Debug>(
        #[case] template: &str,
        #[case] err: E,
    ) {
        let e = DetectorTemplate::new_checked(template).unwrap_err();
        assert_eq!(err, e);
    }
}
