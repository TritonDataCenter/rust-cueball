// Copyright 2019 Joyent, Inc.

use std::error;

/// Cueball connection
///
/// The `Connection` trait defines the interface that must be implemented in
/// order to participate in a Cueball connection pool. A connection need not be
/// limited to a TCP socket, but could be any logical notion of a connection
/// that implements the `Connection` trait.
pub trait Connection: Send + Sized + 'static {
    /// The error type returned by the [`connect`](trait.Connection.html#tymethod.connect) or [`close`](trait.Connection.html#tymethod.close) functions. This
    /// is an associated type for the trait meaning each specific implementation
    /// of the [`Connection`](trait.Connection.html) trait may choose the appropriate concrete error type
    /// to return. The only constraint applied is that the selected error type
    /// must implement the
    /// [Error](https://doc.rust-lang.org/std/error/trait.Error.html) trait from
    /// the standard library. This allows for the error to relevant to the
    /// context of the `Connection` implementation while avoiding unnecessary
    /// type parameters or having to coerce data between incompatible error
    /// types.
    type Error: error::Error;
    /// Attempt to establish the connection to a backend. `Connection` trait
    /// implementors are provided with details about the backend when the
    /// `create_connection` function is invoked by the connection pool upon
    /// notification by the [`Resolver`](../resolver/trait.Resolver.html) that a new backend is available. The
    /// `create_connection` function is provided to the connection pool via the
    /// input parameters to `ConnectionPool::new`. Returns an [`error`](
    /// ../error/enum.Error.html) if the connection attempt fails.
    fn connect(&mut self) -> Result<(), Self::Error>;
    /// check the to see if connection is still up and working. The connection pool runs this
    /// function as the connection is being replaced and triggers a rebalance if the
    /// connection is unhealthy.
    fn is_valid(&mut self) -> bool {
        true
    }
    // Check to see if the connection has closed or is not operational.
    fn has_broken(&self) -> bool {
        false
    }
    /// Close the connection to the backend
    fn close(&mut self) -> Result<(), Self::Error>;
}
