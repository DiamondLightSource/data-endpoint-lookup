use std::fmt::Display;

use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use serde::{Deserialize, Serialize};

const OPA: &'static str = "http://localhost:8181/v1/data/numtracks/state";

#[derive(Debug, Serialize)]
struct Input<'a> {
    input: Request<'a>,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    user: &'a str,
    proposal: usize,
    visit: usize,
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

pub(crate) async fn check(
    token: &Authorization<Bearer>,
    beamline: &str,
    visit: &str,
) -> Result<(), AuthError> {
    let client = reqwest::Client::new();
    let (prop, vis) = visit.split_once('-').ok_or(AuthError::Failed)?;
    let prop = prop
        .chars()
        .skip_while(|p| !p.is_ascii_digit())
        .collect::<String>();

    let query = Input {
        input: Request {
            user: token.token(),
            proposal: prop.parse().map_err(|_| AuthError::Failed)?,
            visit: vis.parse().map_err(|_| AuthError::Failed)?,
        },
    };
    let response = client.post(OPA).json(&query).send().await?;
    let response = response
        .json::<Response>()
        .await
        .map_err(|e| {
            dbg!(e);
            AuthError::Failed
        })?
        .result;
    dbg!(&response);
    if !response.access {
        Err(AuthError::Failed)
    } else if response.beamline != beamline {
        Err(AuthError::BeamlineMismatch {
            expected: beamline.into(),
            actual: response.beamline,
        })
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub enum AuthError {
    ServerError(reqwest::Error),
    Failed,
    BeamlineMismatch { expected: String, actual: String },
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
