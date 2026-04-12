use std::fmt;

/// Errors returned by SkewGuard operations.
#[derive(Debug)]
pub enum Error {
    /// Transaction aborted due to write-write or read-write conflict.
    Conflict,
    /// Storage backend error.
    Storage(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Conflict => write!(f, "transaction aborted: conflict detected"),
            Error::Storage(e) => write!(f, "storage error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
