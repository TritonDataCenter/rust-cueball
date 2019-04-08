/*
 * Copyright 2019 Joyent, Inc.
 */

use crate::backend::Backend;
use crate::error::Error;

/// Cueball connection
///
/// The `Connection` trait defines the interface that must be implemented in
/// order to participate in a Cueball connection pool. A connection need not be
/// limited to a TCP socket, but could be any logical notion of a connection
/// that implements the `Connection` trait.
pub trait Connection: Send + Sync + Sized + 'static {
    /// Returns a new `Connection` instance given a reference to an instance of
    /// `Backend`.
    fn new(b: &Backend) -> Self;
    /// Attempt to establish the connection to the backend provided in the
    /// [`new`]: #method.new call. Returns an [`error`]: ../enum.Error.html if
    /// the connection attempt fails.
    fn connect(&mut self) -> Result<(), Error>;
    /// Close the connection to the backend
    fn close(&mut self) -> Result<(), Error>;
}
