// Copyright 2019 Joyent, Inc.

use std::sync::mpsc::Sender;

use crate::backend;

/// Cueball backend resolver
///
/// [`Resolver`](trait.Resolver.html)s identify the available backends (*i.e.* nodes) providing a
/// service and relay information about those backends to the connection
/// pool. [`Resolver`](trait.Resolver.html)s should also inform the connection pool when a backend is no longer
/// available so that the pool can rebalance the connections on the remaining
/// backends.
pub trait Resolver: Send + 'static {
    /// Start the operation of the resolver. Begin querying for backends and
    /// notifying the connection pool using the provided [`Sender`](https://doc.rust-lang.org/std/sync/mpsc/struct.Sender.html).
    ///
    /// This function is expected to block while the resolver is running, and
    /// return if the receiving end of the channel is closed, or if the Resolver
    /// encounters an unrecoverable error of any sort. Thus, callers can shut
    /// down the resolver by closing the receiving end of the channel.
    fn run(&mut self, s: Sender<BackendMsg>);
}

/// Represents the message that should be sent to the connection pool when a new
/// backend is found.
pub struct BackendAddedMsg {
    /// A backend key
    pub key: backend::BackendKey,
    /// A [`Backend`](../backend/struct.Backend.html) instance
    pub backend: backend::Backend,
}

impl PartialEq for BackendAddedMsg {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for BackendAddedMsg {}

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
    StopMsg,
    // For internal pool use only. Resolver implementations can send this
    // message to test whether or not the channel has been closed.
    #[doc(hidden)]
    HeartbeatMsg,
}

impl PartialEq for BackendMsg {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BackendMsg::AddedMsg(a), BackendMsg::AddedMsg(b)) => a == b,
            (BackendMsg::RemovedMsg(a), BackendMsg::RemovedMsg(b)) => {
                a.0 == b.0
            }
            (BackendMsg::StopMsg, BackendMsg::StopMsg) => true,
            (BackendMsg::HeartbeatMsg, BackendMsg::HeartbeatMsg) => true,
            _ => false,
        }
    }
}

impl Eq for BackendMsg {}

/// Returned from the functions used by the connection pool to add or remove
/// backends based on the receipt of [`BackedMsg`]](enum.BackendAction.html)s by the pool.
pub enum BackendAction {
    /// Indicates a new backend was added by the connection pool.
    BackendAdded,
    /// Indicates an existing backend was removed by the connection pool.
    BackendRemoved,
}
