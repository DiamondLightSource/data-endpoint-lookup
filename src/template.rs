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
use std::fmt::{Debug, Display};
use std::path::{Component, PathBuf};

pub trait FieldSource<F> {
    fn resolve(&self, field: &F) -> Cow<'_, str>;
}

#[derive(Debug, PartialEq, Eq)]
enum Part<Field> {
    Literal(String),
    Field(Field),
}

impl<Field> Part<Field> {
    fn field(&self) -> Option<&Field> {
        match self {
            Part::Literal(_) => None,
            Part::Field(f) => Some(f),
        }
    }
}

#[derive(Debug)]
pub struct Template<Field> {
    parts: Vec<Part<Field>>,
}

impl<F: Display> Display for Template<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for p in &self.parts {
            match p {
                Part::Literal(lit) => f.write_str(lit.as_str())?,
                Part::Field(fld) => write!(f, "{{{fld}}}")?,
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PathTemplate<Field> {
    parts: Vec<Template<Field>>,
    kind: PathType,
}

impl<F: Display> Display for PathTemplate<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.kind == PathType::Absolute {
            f.write_str("/")?;
        }
        let mut parts = self.parts.iter();
        if let Some(p) = parts.next() {
            p.fmt(f)?;
            for p in parts {
                f.write_str("/")?;
                p.fmt(f)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathType {
    Absolute,
    Relative,
}
impl PathType {
    fn init(&self) -> PathBuf {
        match self {
            PathType::Absolute => PathBuf::from("/"),
            PathType::Relative => PathBuf::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PathTemplateError {
    InvalidPath,
    TemplateError(TemplateError),
}

impl Display for PathTemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathTemplateError::InvalidPath => f.write_str("Path is not valid"),
            PathTemplateError::TemplateError(e) => write!(f, "{e}"),
        }
    }
}

impl Error for PathTemplateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PathTemplateError::InvalidPath => None,
            PathTemplateError::TemplateError(e) => Some(e),
        }
    }
}

#[derive(Debug)]
enum ParseState {
    /// We haven't started parsing anything yet
    Init,
    /// We are parsing a field key
    PartialKey(String),
    /// We are parsing a literal section of the template
    Literal(String),
    /// We are reading a literal section of the template but have encountered a (potentially
    /// escaped) opening brace.
    PendingLiteral(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct TemplateError {
    position: usize,
    kind: ErrorKind,
}

impl Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error parsing template: {} at {}",
            self.kind, self.position
        )
    }
}

impl Error for TemplateError {}

/// The reasons why a Template could be invalid
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ErrorKind {
    /// Template placeholders cannot contain other placeholders
    Nested,
    /// Placeholders cannot be empty
    Empty,
    /// A placeholder was opened but not closed
    Incomplete,
    /// The placeholder was not a recognised key
    Unrecognised,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Nested => f.write_str("Nested placeholder"),
            ErrorKind::Empty => f.write_str("Empty placeholder"),
            ErrorKind::Incomplete => f.write_str("Unclosed placeholder"),
            ErrorKind::Unrecognised => f.write_str("Invalid placeholder"),
        }
    }
}

impl TemplateError {
    fn new(position: usize, kind: ErrorKind) -> Self {
        Self { position, kind }
    }
    fn nested(position: usize) -> Self {
        Self::new(position, ErrorKind::Nested)
    }
    fn incomplete(position: usize) -> Self {
        Self::new(position, ErrorKind::Incomplete)
    }
    fn empty(position: usize) -> Self {
        Self::new(position, ErrorKind::Empty)
    }
    fn unknown(position: usize) -> Self {
        Self::new(position, ErrorKind::Unrecognised)
    }
}

impl From<TemplateError> for PathTemplateError {
    fn from(value: TemplateError) -> Self {
        Self::TemplateError(value)
    }
}

impl<F: TryFrom<String>> Template<F> {
    fn new<S: AsRef<str>>(template: S) -> Result<Self, TemplateError> {
        let mut parts = vec![];
        let mut state = ParseState::Init;
        for (i, c) in template.as_ref().chars().enumerate() {
            match c {
                '{' => match state {
                    ParseState::Init => state = ParseState::PartialKey(String::new()),
                    ParseState::PartialKey(_) => return Err(TemplateError::nested(i)),
                    ParseState::Literal(val) => state = ParseState::PendingLiteral(val),
                    ParseState::PendingLiteral(val) => state = ParseState::Literal(val + "{"),
                },
                '}' => match state {
                    ParseState::Init => state = ParseState::Literal("}".into()),
                    ParseState::PartialKey(key) if key.trim().is_empty() => {
                        return Err(TemplateError::empty(i))
                    }
                    ParseState::PartialKey(key) => {
                        match F::try_from(key) {
                            Ok(field) => parts.push(Part::Field(field)),
                            Err(_) => return Err(TemplateError::unknown(i)),
                        }
                        // parts.push(Part::Field(F::try_from(key)));
                        state = ParseState::Init;
                    }
                    ParseState::PendingLiteral(_) => return Err(TemplateError::empty(i)),
                    ParseState::Literal(val) => state = ParseState::Literal(val + "}"),
                },
                c => match state {
                    ParseState::Init => state = ParseState::Literal(c.into()),
                    ParseState::PartialKey(mut key) => {
                        key.push(c);
                        state = ParseState::PartialKey(key);
                    }
                    ParseState::Literal(mut text) => {
                        text.push(c);
                        state = ParseState::Literal(text);
                    }
                    ParseState::PendingLiteral(text) => {
                        parts.push(Part::Literal(text));
                        state = ParseState::PartialKey(c.into());
                    }
                },
            }
        }
        match state {
            ParseState::Init => {}
            ParseState::PendingLiteral(_) | ParseState::PartialKey(_) => {
                return Err(TemplateError::incomplete(template.as_ref().len()))
            }
            ParseState::Literal(text) => parts.push(Part::Literal(text)),
        }
        Ok(Self { parts })
    }
}

impl<F> Template<F> {
    pub fn render<Src: FieldSource<F>>(&self, src: &Src) -> String {
        let mut buf = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(text) => buf.push_str(text),
                Part::Field(f) => buf.push_str(&src.resolve(f)),
            }
        }
        buf
    }
    /// Iterate through all the fields in this template. Fields may be duplicated if they are
    /// referenced multiple times.
    pub fn referenced_fields(&self) -> impl Iterator<Item = &F> {
        self.parts.iter().filter_map(|p| p.field())
    }
}

impl<F: TryFrom<String>> PathTemplate<F> {
    pub(super) fn new<S: AsRef<str>>(template: S) -> Result<Self, PathTemplateError> {
        let path = PathBuf::from(template.as_ref());
        let mut parts = Vec::new();
        let mut kind = PathType::Relative;
        for comp in path.components() {
            match comp {
                Component::Normal(seg) => match seg.to_str() {
                    Some(seg) => parts.push(Template::new(seg)?),
                    None => return Err(PathTemplateError::InvalidPath),
                },
                Component::RootDir => kind = PathType::Absolute,
                Component::CurDir => continue,
                _ => return Err(PathTemplateError::InvalidPath),
            }
        }
        Ok(Self { parts, kind })
    }
}

impl<F> PathTemplate<F> {
    pub fn render<Src>(&self, src: &Src) -> PathBuf
    where
        Src: FieldSource<F>,
    {
        let mut path = self.kind.init();
        for part in &self.parts {
            path.push(part.render(src));
        }
        path
    }

    pub fn is_absolute(&self) -> bool {
        self.kind == PathType::Absolute
    }

    /// Iterate through all the fields in this path. Fields may be duplicated if they are
    /// referenced multiple times in the path.
    pub fn referenced_fields(&self) -> impl Iterator<Item = &F> {
        self.parts.iter().flat_map(|t| t.referenced_fields())
    }
}

#[cfg(test)]
mod parser_tests {
    use super::Part::*;
    use super::*;

    type StrTemplate = Template<String>;

    fn literal(lit: &'static str) -> Part<String> {
        Part::Literal(lit.into())
    }
    fn field(f: &'static str) -> Part<String> {
        Part::Field(f.into())
    }

    #[test]
    fn only_literal() {
        let temp = Template::new("this is all literal").unwrap();
        assert_eq!(temp.parts, vec![literal("this is all literal")])
    }

    #[test]
    fn only_single_field() {
        let temp = StrTemplate::new("{year}").unwrap();
        assert_eq!(temp.parts, vec![Field("year".into())]);
    }

    #[test]
    fn only_fields() {
        let temp = StrTemplate::new("{year}{visit}{proposal}").unwrap();
        assert_eq!(
            temp.parts,
            vec![field("year"), field("visit"), field("proposal")]
        );
    }

    #[test]
    fn mixed_literal_and_fields() {
        // Start/end with literal
        let temp = StrTemplate::new("start{visit}middle{year}end").unwrap();
        assert_eq!(
            temp.parts,
            vec![
                literal("start"),
                field("visit"),
                literal("middle"),
                field("year"),
                literal("end")
            ]
        );

        // Start/end with field
        let temp = StrTemplate::new("{year}first{visit}second{proposal}").unwrap();
        assert_eq!(
            temp.parts,
            vec![
                field("year"),
                literal("first"),
                field("visit"),
                literal("second"),
                field("proposal")
            ]
        )
    }

    #[test]
    fn escaped_open() {
        let temp = StrTemplate::new("all {{ literal").unwrap();
        assert_eq!(temp.parts, vec![literal("all { literal")])
    }

    macro_rules! error {
        ($pos:literal, $kind:ident) => {
            TemplateError {
                position: $pos,
                kind: ErrorKind::$kind,
            }
        };
    }

    #[test]
    fn empty_key() {
        let temp = StrTemplate::new("missing {} key").unwrap_err();
        assert_eq!(temp, error!(9, Empty));

        let temp = StrTemplate::new("whitespace {  } key").unwrap_err();
        assert_eq!(temp, error!(14, Empty));
    }

    #[test]
    fn unmatched_close() {
        let temp = StrTemplate::new("closing } only").unwrap();
        assert_eq!(temp.parts, vec![literal("closing } only")]);

        let temp = StrTemplate::new("} closing start").unwrap();
        assert_eq!(temp.parts, vec![literal("} closing start")]);

        let temp = StrTemplate::new("double {close}}").unwrap();
        assert_eq!(
            temp.parts,
            vec![literal("double "), field("close"), literal("}")]
        )
    }

    #[test]
    fn nested_keys() {
        let temp = StrTemplate::new("{nested{keys}}").unwrap_err();
        assert_eq!(temp, error!(7, Nested))
    }

    #[test]
    fn incomplete_key() {
        let temp = StrTemplate::new("incomplete {key").unwrap_err();
        assert_eq!(temp, error!(15, Incomplete));

        let temp = StrTemplate::new("incomplete {").unwrap_err();
        assert_eq!(temp, error!(12, Incomplete));
    }
}

#[cfg(test)]
mod string_templates {
    use super::*;
    pub type StrTemplate = Template<String>;

    /// FieldSource that replaces every key with the uppercase version of itself
    pub struct EchoSource;

    impl FieldSource<String> for EchoSource {
        fn resolve(&self, field: &String) -> Cow<'_, str> {
            field.to_uppercase().into()
        }
    }

    /// Field Source that replaces every key with the empty string
    pub struct NullSource;
    impl FieldSource<String> for NullSource {
        fn resolve(&self, _: &String) -> Cow<'_, str> {
            Cow::Owned(String::new())
        }
    }
}

#[cfg(test)]
mod template_tests {
    use super::string_templates::*;

    #[test]
    fn literal_template() {
        let text = StrTemplate::new("all literal").unwrap().render(&EchoSource);
        assert_eq!(text, "all literal");
    }

    #[test]
    fn mixed() {
        let text = StrTemplate::new("/tmp/{instrument}/data/{year}/{visit}/")
            .unwrap()
            .render(&EchoSource);
        assert_eq!(text, "/tmp/INSTRUMENT/data/YEAR/VISIT/");
    }
}

#[cfg(test)]
mod path_template_tests {
    use super::string_templates::*;
    use super::*;

    fn from_template<Src: FieldSource<String>>(fmt: &'static str, src: &Src) -> PathBuf {
        PathTemplate::new(fmt).unwrap().render(src)
    }

    #[test]
    fn literal_absolute_path() {
        let path = from_template("/absolute/literal/path", &EchoSource);
        assert_eq!(path, PathBuf::from("/absolute/literal/path"))
    }

    #[test]
    fn templated_absolute_path() {
        let path = from_template("/path/{with}/mixed_{fields}/", &EchoSource);
        assert_eq!(path, PathBuf::from("/path/WITH/mixed_FIELDS/"))
    }

    #[test]
    fn absolute_empty_segment() {
        let path = from_template("/with/{optional}/parts", &NullSource);
        assert_eq!(path, PathBuf::from("/with/parts"))
    }

    #[test]
    fn literal_relative() {
        let path = from_template("relative/literal/path", &EchoSource);
        assert_eq!(path, PathBuf::from("relative/literal/path"));

        let path = from_template("./relative/literal/path", &EchoSource);
        assert_eq!(path, PathBuf::from("relative/literal/path"));
    }

    #[test]
    fn dynamic_relative() {
        let path = from_template("relative/{literal}/path", &EchoSource);
        assert_eq!(path, PathBuf::from("relative/LITERAL/path"));

        let path = from_template("{opening}_dynamic/path", &EchoSource);
        assert_eq!(path, PathBuf::from("OPENING_dynamic/path"));
    }

    #[test]
    fn missing_relative_opener() {
        let path = from_template("{optional}/relative/opener", &NullSource);
        assert_eq!(path, PathBuf::from("relative/opener"))
    }

    #[test]
    fn partial_opener() {
        let path = from_template("{optional}_part/of/relative", &NullSource);
        assert_eq!(path, PathBuf::from("_part/of/relative"))
    }

    #[test]
    fn invalid_path() {
        fn assert_invalid(fmt: &'static str) {
            assert_eq!(
                PathTemplate::<String>::new(fmt).unwrap_err(),
                PathTemplateError::InvalidPath
            )
        }
        assert_invalid("../empty/{segment}");
        assert_invalid("/../empty/{segment}");
    }
}
