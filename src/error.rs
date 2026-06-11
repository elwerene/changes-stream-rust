use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Could not parse the url: {0:?}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("Request failed: {0:?}")]
    RequestFailed(#[from] reqwest::Error),
    #[error("Server answered with non-ok status: {status}. body: {body}")]
    InvalidResponse {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("Could not parse server response: {json}")]
    ParsingFailed {
        #[source]
        error: serde_json::Error,
        json: String,
    },
}
