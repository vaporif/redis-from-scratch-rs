use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("unknown command")]
    UnknownCommand,
    #[error("connection issue")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ParseError(#[from] serde_resp::Error),
    #[error("response error")]
    ResponseError(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
