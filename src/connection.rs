/*
 * Copyright 2019 Joyent, Inc.
 */

use crate::error::Error;

/// Cueball connection
///
/// The `Connection` trait defines the interface that must be implemented in
/// order to participate in a Cueball connection pool. A connection need not be
/// limited to a TCP socket, but could be any logical notion of a connection
/// that implements the `Connection` trait.
pub trait Connection: Send + Sized + 'static {
    /// Attempt to establish the connection to a backend. `Connection` trait
    /// implementors are provided with details about the backend when the
    /// `create_connection` function is invoked by the connection pool upon
    /// notification by the `Resolver` that a new backend is available. The
    /// `create_connection` function is provided to the connection pool via the
    /// input parameters to `ConnectionPool::new`. Returns an [`error`]:
    /// ../enum.Error.html if the connection attempt fails.
    fn connect(&mut self) -> Result<(), Error>;
    /// Close the connection to the backend
    fn close(&mut self) -> Result<(), Error>;
}
