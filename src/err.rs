use std::fmt::Display;

// Public
#[derive(Debug)]
pub enum ErrorKind {
    Other,
    BadConnection,
    InvalidScript,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    msg: String,
}

impl Error {
    pub fn new(kind: ErrorKind, msg: String) -> Error {
        Error { kind, msg }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.msg)
    }
}

pub type Result<T> = core::result::Result<T, Error>;
