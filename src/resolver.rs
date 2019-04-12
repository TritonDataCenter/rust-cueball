/*
 * Copyright 2019 Joyent, Inc.
 */

use std::sync::mpsc::Sender;

use crate::backend;

/// Cueball backend resolver
///
/// `Resolver`s identify the available backends (*i.e.* nodes) providing a
/// service and relay information about those backends to the connection
/// pool. It should also inform the connection pool when a backend is no longer
/// available so that the pool can rebalance the connections on the remaining
/// backends.
pub trait Resolver: Send + 'static {
    /// Start the operation of the resolver. Begin querying for backends and
    /// notifying the connection pool using the provided `Sender`.
    fn start(&mut self, s: Sender<BackendMsg>);
    /// Shutdown the resolver. Cease querying for new backends. In the event
    /// that attempting to send a message on the `Sender` channel provided in
    /// [`start`]: #method.start fails with an error then this method should be
    /// called as it indicates the connection pool is shutting down.
    fn stop(&mut self);
    /// Return the last error if one has occurred.
    fn get_last_error(&self) -> Option<String>;
}

/// Represents the message that should be sent to the connection pool when a new
/// backend is found.
pub struct BackendAddedMsg {
    /// A backend key
    pub key: backend::BackendKey,
    /// A `Backend` instance
    pub backend: backend::Backend,
}

/// Represents the message that should be sent to the backend when a backend is
/// no longer available.
pub struct BackendRemovedMsg(pub backend::BackendKey);

/// The types of messages that may be sent to the connection pool. `StopMsg` is
/// only for use by the connection pool when performing cleanup prior to
/// shutting down.
pub enum BackendMsg {
    /// Indicates a new backend was found by the resolver
    AddedMsg(BackendAddedMsg),
    /// Indicates a backend is no longer available to service connections
    RemovedMsg(BackendRemovedMsg),
    // For internal pool use only
    #[doc(hidden)]
    StopMsg
}

/// Returned from the functions used by the connection pool to add or remove
/// backends based on the receipt of `BackedMsg`s by the pool.
pub enum BackendAction {
    /// Indicates a new backend was added by the connection pool.
    BackendAdded,
    /// Indicates an existing backend was removed by the connection pool.
    BackendRemoved
}
