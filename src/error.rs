/*
 * Copyright 2019 Joyent, Inc.
 */

pub enum Error {
    CueballError(String),
    IOError(std::io::Error)
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IOError(error)
    }
}
