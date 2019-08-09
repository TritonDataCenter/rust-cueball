// Copyright 2019 Joyent, Inc.

use std::error::Error as StdError;
use std::fmt;

const CLAIM_FAILURE_STR: &str = "Unable to retrieve a connection within the \
                                 claim timeout";
const BACKEND_NO_CONNECTION_STR: &str = "Found a backend key with no \
                                         associated connection";
const CONNECTION_RETRIEVAL_FAILURE_STR: &str = "Unable to retrieve a \
                                                connection";
const STOP_CALLED_BY_CLONE_STR: &str =
    "ConnectionPool clones may not stop the \
     connection pool.";
const DUMMY_ERROR_STR: &str = "dummy error";

#[derive(Debug)]
/// The cueball `Error` type is an `enum` that represents the different errors
/// that may be returned by the cueball API.
pub enum Error {
    /// The call to `claim` to failed to retrieve a connection within the specified timeout period.
    ClaimFailure,
    /// The `stop` function was called on a pool clone. Only the original
    /// connction pool instance may stop a connection pool. Thread `JoinHandles`
    /// may not be cloned and therefore invocation of this function by a clone
    /// of the pool results in an error.
    StopCalledByClone,
    /// A backend key was found with no associated connection. This error should
    /// never happen and is only represented for completeness. Please file a bug
    /// if it is encountered.
    BackendWithNoConnection,
    /// A connection could not be retrieved from the connection pool even though
    /// the connection pool accounting indicated one should be available. This
    /// error should never happen and is only represented for
    /// completeness. Please file a bug if it is encountered.
    ConnectionRetrievalFailure,
    // For internal pool use only
    #[doc(hidden)]
    DummyError,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ClaimFailure => CLAIM_FAILURE_STR.fmt(fmt),
            Error::BackendWithNoConnection => {
                BACKEND_NO_CONNECTION_STR.fmt(fmt)
            }
            Error::ConnectionRetrievalFailure => {
                CONNECTION_RETRIEVAL_FAILURE_STR.fmt(fmt)
            }
            Error::StopCalledByClone => STOP_CALLED_BY_CLONE_STR.fmt(fmt),
            Error::DummyError => DUMMY_ERROR_STR.fmt(fmt),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match self {
            Error::ClaimFailure => CLAIM_FAILURE_STR,
            Error::BackendWithNoConnection => BACKEND_NO_CONNECTION_STR,
            Error::ConnectionRetrievalFailure => {
                CONNECTION_RETRIEVAL_FAILURE_STR
            }
            Error::StopCalledByClone => STOP_CALLED_BY_CLONE_STR,
            Error::DummyError => DUMMY_ERROR_STR,
        }
    }

    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}
