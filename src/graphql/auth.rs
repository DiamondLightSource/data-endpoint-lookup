use std::fmt::Display;
use std::str::FromStr;

use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use serde::{Deserialize, Serialize};

const AUDIENCE: &str = "account";

#[derive(Debug, Serialize)]
struct Input<'a> {
    input: Request<'a>,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    token: &'a str,
    audience: &'a str,
    proposal: u32,
    visit: u16,
}

#[derive(Debug, Deserialize)]
struct Response {
    result: Decision,
}

#[derive(Debug, Serialize, Deserialize)]
struct Decision {
    access: bool,
    beamline: String,
}

#[derive(Debug)]
struct InvalidVisit;
struct Visit(String, u32, u16);
impl FromStr for Visit {
    type Err = InvalidVisit;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (code_prop, vis) = s.split_once('-').ok_or(InvalidVisit)?;
        let prop = code_prop
            .chars()
            .skip_while(|p| !p.is_ascii_digit())
            .collect::<String>();
        let prop = prop.parse().map_err(|_| InvalidVisit)?;
        let vis = vis.parse().map_err(|_| InvalidVisit)?;
        let code = code_prop
            .chars()
            .take_while(|p| !p.is_ascii_digit())
            .collect();
        Ok(Self(code, prop, vis))
    }
}

pub(crate) struct PolicyCheck(reqwest::Client, String);

impl PolicyCheck {
    pub fn new<S: Into<String>>(endpoint: S) -> Self {
        Self(reqwest::Client::new(), endpoint.into())
    }
    pub async fn check(
        &self,
        token: Option<&Authorization<Bearer>>,
        beamline: &str,
        visit: &str,
    ) -> Result<(), AuthError> {
        let token = token.ok_or(AuthError::Missing)?;
        let visit: Visit = visit.parse().map_err(|_| AuthError::Failed)?;
        let query = Input {
            input: Request {
                token: token.token(),
                audience: AUDIENCE,
                proposal: visit.1,
                visit: visit.2,
            },
        };
        let response = self.0.post(&self.1).json(&query).send().await?;
        let response = response
            .json::<Response>()
            .await
            .map_err(|_| AuthError::Failed)?
            .result;
        if !response.access {
            Err(AuthError::Failed)
        } else if response.beamline != beamline {
            Err(AuthError::BeamlineMismatch {
                actual: response.beamline,
                expected: beamline.to_string(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum AuthError {
    ServerError(reqwest::Error),
    Failed,
    BeamlineMismatch { expected: String, actual: String },
    Missing,
}

impl Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::ServerError(e) => e.fmt(f),
            AuthError::Failed => write!(f, "Authentication failed"),
            AuthError::BeamlineMismatch { expected, actual } => write!(
                f,
                "Invalid beamline. Expected: {expected}, actual: {actual}"
            ),
            AuthError::Missing => f.write_str("No authenication token was provided"),
        }
    }
}

impl std::error::Error for AuthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AuthError::ServerError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for AuthError {
    fn from(value: reqwest::Error) -> Self {
        Self::ServerError(value)
    }
}