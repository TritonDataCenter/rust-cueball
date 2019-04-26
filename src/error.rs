/*
 * Copyright 2019 Joyent, Inc.
 */

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
