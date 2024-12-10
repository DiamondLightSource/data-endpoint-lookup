use std::fmt::Display;
use std::str::FromStr;

use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::cli::PolicyOptions;

const AUDIENCE: &str = "account";

type Token = Authorization<Bearer>;

#[derive(Debug, Deserialize)]
struct Response {
    result: bool,
}

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(Deserialize))]
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
#[cfg_attr(test, derive(Deserialize))]
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
        info!(
            "Checking authorization against {:?} using {:?} for admin and {:?} for access",
            endpoint.policy_host, endpoint.admin_query, endpoint.visit_query
        );
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

    async fn authorise(&self, query: &str, input: impl Serialize) -> Result<(), AuthError> {
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
            AuthError::ServerError(_) => write!(f, "Invalid authorization configuration"),
            AuthError::Failed => write!(f, "Authentication failed"),
            AuthError::Missing => f.write_str("No authentication token was provided"),
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

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use axum::http::HeaderValue;
    use axum_extra::headers::authorization::{Bearer, Credentials};
    use axum_extra::headers::Authorization;
    use httpmock::MockServer;
    use rstest::rstest;

    use super::{
        AccessRequest, AdminRequest, AuthError, InvalidVisit, PolicyCheck, Visit, AUDIENCE,
    };
    use crate::cli::PolicyOptions;

    fn token(name: &'static str) -> Option<Authorization<Bearer>> {
        Some(Authorization(
            Bearer::decode(&HeaderValue::from_str(&format!("Bearer {name}")).unwrap()).unwrap(),
        ))
    }

    #[test]
    fn valid_visit() {
        let visit = Visit::from_str("cm12345-1").unwrap();
        assert_eq!(visit.session, 1);
        assert_eq!(visit.proposal, 12345);
    }

    #[rstest]
    #[case::no_proposal("cm-3")]
    #[case::no_session("cm12345")]
    #[case::invalid_session("cm12345-abc")]
    #[case::invalid_proposal("cm123abc-12")]
    #[case::negative_session("cm1234--12")]
    fn invalid_visit(#[case] visit: &str) {
        assert!(matches!(Visit::from_str(visit), Err(InvalidVisit)))
    }

    #[tokio::test]
    async fn successful_access_check() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/demo/access")
                    .json_body_obj(&AccessRequest {
                        token: "token",
                        beamline: "i22",
                        visit: 4,
                        proposal: 1234,
                        audience: AUDIENCE,
                    });
                then.status(200).body(r#"{"result": true}"#);
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });
        check
            .check_access(token("token").as_ref(), "i22", "cm1234-4")
            .await
            .unwrap();
        mock.assert();
    }

    #[tokio::test]
    async fn successful_admin_check() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/demo/admin")
                    .json_body_obj(&AdminRequest {
                        token: "token",
                        beamline: "i22",
                        audience: AUDIENCE,
                    });
                then.status(200).body(r#"{"result": true}"#);
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });
        check
            .check_admin(token("token").as_ref(), "i22")
            .await
            .unwrap();
        mock.assert();
    }

    #[tokio::test]
    async fn denied_access_check() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/demo/access")
                    .json_body_obj(&AccessRequest {
                        token: "token",
                        beamline: "i22",
                        proposal: 1234,
                        visit: 4,
                        audience: AUDIENCE,
                    });
                then.status(200).body(r#"{"result": false}"#);
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });

        let result = check
            .check_access(token("token").as_ref(), "i22", "cm1234-4")
            .await;
        let Err(AuthError::Failed) = result else {
            panic!("Unexpected result from unauthorised check: {result:?}");
        };
        mock.assert();
    }

    #[tokio::test]
    async fn denied_admin_check() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/demo/admin")
                    .json_body_obj(&AdminRequest {
                        token: "token",
                        beamline: "i22",
                        audience: AUDIENCE,
                    });
                then.status(200).body(r#"{"result": false}"#);
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });
        let result = check.check_admin(token("token").as_ref(), "i22").await;
        let Err(AuthError::Failed) = result else {
            panic!("Unexpected result from unauthorised check: {result:?}");
        };
        mock.assert();
    }

    #[tokio::test]
    async fn unauthorised_access_check() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|_, _| {
                // mock that rejects every request
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });
        let result = check.check_access(None, "i22", "cm1234-4").await;
        let Err(AuthError::Missing) = result else {
            panic!("Unexpected result from unauthorised check: {result:?}");
        };
        mock.assert_hits(0);
    }

    #[tokio::test]
    async fn unauthorised_admin_check() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|_, _| {
                // mock that rejects every request
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });
        let result = check.check_admin(None, "i22").await;
        let Err(AuthError::Missing) = result else {
            panic!("Unexpected result from unauthorised check: {result:?}");
        };
        mock.assert_hits(0);
    }

    #[tokio::test]
    async fn server_error() {
        let server = MockServer::start();
        let mock = server
            .mock_async(|when, then| {
                when.method("POST");
                then.status(503);
            })
            .await;
        let check = PolicyCheck::new(PolicyOptions {
            policy_host: server.url(""),
            visit_query: "demo/access".into(),
            admin_query: "demo/admin".into(),
        });
        let result = check.check_admin(token("token").as_ref(), "i22").await;
        let Err(AuthError::ServerError(_)) = result else {
            panic!("Unexpected result from unauthorised check: {result:?}");
        };
        mock.assert();
    }
}
