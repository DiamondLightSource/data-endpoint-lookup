use std::fmt::Display;
use std::str::FromStr;

use access::Request as AccessReq;
use admin::Request as AdminReq;
use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::cli::PolicyOptions;

const AUDIENCE: &str = "account";

type Token = Authorization<Bearer>;

#[derive(Debug, Serialize)]
struct Query<'q, I> {
    query: &'q str,
    input: I,
}

#[derive(Debug, Deserialize)]
struct Response<R> {
    result: R,
}

trait Input: Serialize {
    type Response: Auth;
}
trait Auth: DeserializeOwned {
    fn check(self) -> Result<(), AuthError>;
}

mod access {
    use serde::{Deserialize, Serialize};

    use super::{Auth, AuthError, Input, Token, Visit, AUDIENCE};

    #[derive(Debug, Serialize)]
    pub struct Request<'a> {
        token: &'a str,
        audience: &'a str,
        proposal: u32,
        visit: u16,
        beamline: &'a str,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Decision {
        access: bool,
        beamline_matches: bool,
    }

    impl<'a> Request<'a> {
        pub fn new(
            token: Option<&'a Token>,
            visit: Visit,
            beamline: &'a str,
        ) -> Result<Self, AuthError> {
            Ok(Self {
                token: token.ok_or(AuthError::Missing)?.token(),
                audience: AUDIENCE,
                proposal: visit.proposal,
                visit: visit.session,
                beamline,
            })
        }
    }

    impl Input for Request<'_> {
        type Response = Decision;
    }

    impl Auth for Decision {
        fn check(self) -> Result<(), super::AuthError> {
            if !self.access {
                Err(AuthError::Failed)
            } else if !self.beamline_matches {
                Err(AuthError::BeamlineMismatch)
            } else {
                Ok(())
            }
        }
    }
}

mod admin {
    use serde::{Deserialize, Serialize};

    use super::{Auth, AuthError, Input, Token, AUDIENCE};

    #[derive(Debug, Serialize)]
    pub struct Request<'a> {
        token: &'a str,
        audience: &'a str,
        beamline: &'a str,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Decision {
        beamline_admin: bool,
        admin: bool,
    }

    impl Input for Request<'_> {
        type Response = Decision;
    }

    impl<'r> Request<'r> {
        pub(crate) fn new(token: Option<&'r Token>, beamline: &'r str) -> Result<Self, AuthError> {
            Ok(Self {
                token: token.ok_or(AuthError::Missing)?.token(),
                audience: AUDIENCE,
                beamline,
            })
        }
    }

    impl Auth for Decision {
        fn check(self) -> Result<(), AuthError> {
            if self.beamline_admin || self.admin {
                Ok(())
            } else {
                Err(AuthError::Failed)
            }
        }
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
    /// OPA instance
    host: String,
    /// Rego query for getting admin rights
    admin: String,
    /// Rego query for getting access rights
    access: String,
}

impl PolicyCheck {
    pub fn new(endpoint: PolicyOptions) -> Self {
        Self {
            client: reqwest::Client::new(),
            host: endpoint.host + "/v1/query",
            admin: endpoint.admin_query,
            access: endpoint.visit_query,
        }
    }
    pub async fn check_access(
        &self,
        token: Option<&Authorization<Bearer>>,
        beamline: &str,
        visit: &str,
    ) -> Result<(), AuthError> {
        let visit: Visit = visit.parse().map_err(|_| AuthError::Failed)?;
        self.authorise(&self.access, AccessReq::new(token, visit, beamline)?)
            .await
    }

    pub async fn check_admin(
        &self,
        token: Option<&Authorization<Bearer>>,
        beamline: &str,
    ) -> Result<(), AuthError> {
        self.authorise(&self.admin, AdminReq::new(token, beamline)?)
            .await
    }

    async fn authorise<'q, I: Input>(&self, query: &'q str, input: I) -> Result<(), AuthError> {
        let response = self
            .client
            .post(&self.host)
            .json(&Query { query, input })
            .send()
            .await?;
        response
            .json::<Response<I::Response>>()
            .await?
            .result
            .check()
    }
}

#[derive(Debug)]
pub enum AuthError {
    ServerError(reqwest::Error),
    Failed,
    BeamlineMismatch,
    Missing,
}

impl Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::ServerError(e) => e.fmt(f),
            AuthError::Failed => write!(f, "Authentication failed"),
            AuthError::BeamlineMismatch => {
                f.write_str("Invalid beamline. Visit is not on current beamline")
            }
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
