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
    prop: i64,
    session: i64,
    user: String,
    visit: i64,
}

pub(crate) async fn check(
    token: &Authorization<Bearer>,
    beamline: &str,
    visit: &str,
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let (prop, vis) = visit.split_once('-').unwrap();
    let prop = prop
        .chars()
        .skip_while(|p| !p.is_ascii_digit())
        .collect::<String>();

    let response = client
        .post(OPA)
        .json(&Input {
            input: Request {
                user: token.token(),
                proposal: prop.parse().unwrap(),
                visit: vis.parse().unwrap(),
            },
        })
        .send()
        .await
        .unwrap();
    // dbg!(response.text().await);
    let response = dbg!(response.json::<Response>().await).unwrap().result;
    if response.beamline != beamline {
        return Err("Incorrect beamline".into());
    }
    Ok(())
}
