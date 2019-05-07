/*
 * Copyright 2019 Joyent, Inc.
 */

use std::error::Error as StdError;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    CueballError(String),
    IOError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IOError(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::CueballError(err_str) => err_str.fmt(fmt),
            Error::IOError(io_err) => io_err.fmt(fmt),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match self {
            Error::CueballError(desc) => desc.as_str(),
            Error::IOError(io_err) => io_err.description()
        }
    }

    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::CueballError(_) => None,
            Error::IOError(io_err) => Some(io_err)
        }
    }
}
