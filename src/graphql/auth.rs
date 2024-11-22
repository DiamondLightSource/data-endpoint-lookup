use std::fmt::Display;
use std::str::FromStr;

use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use serde::{Deserialize, Serialize};

use crate::cli::PolicyOptions;

const AUDIENCE: &str = "account";

type Token = Authorization<Bearer>;

#[derive(Debug, Deserialize)]
struct Response {
    result: bool,
}

#[derive(Debug, Serialize)]
pub struct AccessRequest<'a> {
    token: &'a str,
    audience: &'a str,
    proposal: u32,
    visit: u16,
    beamline: &'a str,
}

impl<'a> AccessRequest<'a> {
    fn new(token: Option<&'a Token>, visit: Visit, beamline: &'a str) -> Result<Self, AuthError> {
        Ok(Self {
            token: token.ok_or(AuthError::Missing)?.token(),
            audience: AUDIENCE,
            proposal: visit.proposal,
            visit: visit.session,
            beamline,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct AdminRequest<'a> {
    token: &'a str,
    audience: &'a str,
    beamline: &'a str,
}

impl<'r> AdminRequest<'r> {
    fn new(token: Option<&'r Token>, beamline: &'r str) -> Result<Self, AuthError> {
        Ok(Self {
            token: token.ok_or(AuthError::Missing)?.token(),
            audience: AUDIENCE,
            beamline,
        })
    }
}

#[derive(Debug)]
struct InvalidVisit;
struct Visit {
    proposal: u32,
    session: u16,
}
impl FromStr for Visit {
    type Err = InvalidVisit;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (code_prop, vis) = s.split_once('-').ok_or(InvalidVisit)?;
        let prop = code_prop
            .chars()
            .skip_while(|p| !p.is_ascii_digit())
            .collect::<String>();
        let proposal = prop.parse().map_err(|_| InvalidVisit)?;
        let session = vis.parse().map_err(|_| InvalidVisit)?;
        Ok(Self { proposal, session })
    }
}

pub(crate) struct PolicyCheck {
    client: reqwest::Client,
    /// Rego query for getting admin rights
    admin: String,
    /// Rego query for getting access rights
    access: String,
}

impl PolicyCheck {
    pub fn new(endpoint: PolicyOptions) -> Self {
        Self {
            client: reqwest::Client::new(),
            admin: format!("{}/{}", endpoint.policy_host, endpoint.admin_query),
            access: format!("{}/{}", endpoint.policy_host, &endpoint.visit_query),
        }
    }
    pub async fn check_access(
        &self,
        token: Option<&Authorization<Bearer>>,
        beamline: &str,
        visit: &str,
    ) -> Result<(), AuthError> {
        let visit: Visit = visit.parse().map_err(|_| AuthError::Failed)?;
        self.authorise(&self.access, AccessRequest::new(token, visit, beamline)?)
            .await
    }

    pub async fn check_admin(
        &self,
        token: Option<&Authorization<Bearer>>,
        beamline: &str,
    ) -> Result<(), AuthError> {
        self.authorise(&self.admin, AdminRequest::new(token, beamline)?)
            .await
    }

    async fn authorise<'q>(&self, query: &'q str, input: impl Serialize) -> Result<(), AuthError> {
        let response = self.client.post(query).json(&input).send().await?;
        if response.json::<Response>().await?.result {
            Ok(())
        } else {
            Err(AuthError::Failed)
        }
    }
}

#[derive(Debug)]
pub enum AuthError {
    ServerError(reqwest::Error),
    Failed,
    Missing,
}

impl Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::ServerError(e) => e.fmt(f),
            AuthError::Failed => write!(f, "Authentication failed"),
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
