// Copyright 2019 Joyent, Inc.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use derive_more::{Add, AddAssign, Display, From, Into, Sub, SubAssign};
use slog::Logger;

use crate::backend::{Backend, BackendKey};
use crate::connection::Connection;

/// The connection counts for the connection pool
#[derive(Copy, Clone, Debug)]
pub struct ConnectionPoolStats {
    /// The total number of connections
    pub total_connections: ConnectionCount,
    /// The count of idle connections in the pool
    pub idle_connections: ConnectionCount,
    /// The number of created, but not yet connected connections
    pub pending_connections: ConnectionCount,
}

impl ConnectionPoolStats {
    /// Create a new instance of `ConnectionPooStats`
    pub fn new() -> Self {
        ConnectionPoolStats {
            total_connections: ConnectionCount::from(0),
            idle_connections: ConnectionCount::from(0),
            pending_connections: ConnectionCount::from(0),
        }
    }
}

impl Default for ConnectionPoolStats {
    fn default() -> Self {
        Self::new()
    }
}

/// The configuration options for a Cueball connection pool. This is required to
/// instantiate a new connection pool.
#[derive(Debug)]
pub struct ConnectionPoolOptions {
    /// An optional maximum number of connections to maintain in the connection
    /// pool. If not specified the default is 10.
    pub max_connections: Option<u32>,
    /// An optional timeout for blocking calls (`claim`) to request a connection
    /// from the pool. If not specified the calls will block indefinitely.
    pub claim_timeout: Option<u64>,
    /// An optional `slog` logger instance. If none is provided then the logging
    /// will fall back to using the [`slog-stdlog`](https://docs.rs/slog-stdlog)
    /// drain which is essentially the same as using the rust standard
    /// [`log`](https://docs.rs/log) crate.
    pub log: Option<Logger>,
    /// An optional delay time to avoid extra rebalancing work in case the
    /// resolver notifies the pool of multiple changes within a short
    /// period. The default is 100 milliseconds.
    pub rebalancer_action_delay: Option<u64>,
    /// Optional decoherence interval in seconds. This represents the length of
    /// the period of the decoherence shuffle. If not specified the default is
    /// 300 seconds.
    pub decoherence_interval: Option<u64>,
    /// Optional connection check interval in seconds. This represents the length of
    /// the period of the pool connection check task. If not specified the default is
    /// 30 seconds.
    pub connection_check_interval: Option<u64>,
}

// This type wraps a pair that associates a `BackendKey` with a connection of
// type `C`. The second member of the pair is an Option type to facilitate
// ownership issues when the connection needs to be closed by the connection
// pool.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ConnectionKeyPair<C>(pub (BackendKey, Option<C>));

impl<C> PartialEq for ConnectionKeyPair<C>
where
    C: Connection,
{
    fn eq(&self, other: &ConnectionKeyPair<C>) -> bool {
        (self.0).0 == (other.0).0
    }
}

impl<C> Eq for ConnectionKeyPair<C> where C: Connection {}

impl<C> Ord for ConnectionKeyPair<C>
where
    C: Connection,
{
    fn cmp(&self, other: &ConnectionKeyPair<C>) -> Ordering {
        (self.0).0.cmp(&(other.0).0)
    }
}

impl<C> PartialOrd for ConnectionKeyPair<C>
where
    C: Connection,
{
    fn partial_cmp(&self, other: &ConnectionKeyPair<C>) -> Option<Ordering> {
        (self.0).0.partial_cmp(&(other.0).0)
    }
}

impl<C> From<(BackendKey, C)> for ConnectionKeyPair<C>
where
    C: Connection,
{
    fn from(pair: (BackendKey, C)) -> Self {
        ConnectionKeyPair((pair.0, Some(pair.1)))
    }
}

impl<C> From<(BackendKey, Option<C>)> for ConnectionKeyPair<C>
where
    C: Connection,
{
    fn from(pair: (BackendKey, Option<C>)) -> Self {
        ConnectionKeyPair((pair.0, pair.1))
    }
}

impl<C> Into<(BackendKey, Option<C>)> for ConnectionKeyPair<C>
where
    C: Connection,
{
    fn into(self) -> (BackendKey, Option<C>) {
        ((self.0).0, (self.0).1)
    }
}

/// A newtype wrapper around u32 used for counts of connections mainatained by the connection pool.
#[derive(
    Add,
    AddAssign,
    Clone,
    Copy,
    Debug,
    Display,
    Eq,
    From,
    Into,
    Ord,
    PartialOrd,
    PartialEq,
    Sub,
    SubAssign,
)]
pub struct ConnectionCount(u32);

// The internal data structures used to manage the connection pool.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ConnectionData<C> {
    pub backends: HashMap<BackendKey, Backend>,
    pub connections: VecDeque<ConnectionKeyPair<C>>,
    pub connection_distribution: HashMap<BackendKey, ConnectionCount>,
    pub unwanted_connection_counts: HashMap<BackendKey, ConnectionCount>,
    pub stats: ConnectionPoolStats,
}

impl<C> ConnectionData<C>
where
    C: Connection,
{
    #[doc(hidden)]
    pub fn new(max_size: usize) -> Self {
        ConnectionData {
            backends: HashMap::with_capacity(max_size),
            connections: VecDeque::with_capacity(max_size),
            connection_distribution: HashMap::with_capacity(max_size),
            unwanted_connection_counts: HashMap::with_capacity(max_size),
            stats: ConnectionPoolStats::new(),
        }
    }
}

// Protected access to the internal connection pool data structures
#[doc(hidden)]
#[derive(Debug)]
pub struct ProtectedData<C>(Arc<(Mutex<ConnectionData<C>>, Condvar)>);

impl<C> ProtectedData<C>
where
    C: Connection,
{
    pub fn new(connection_data: ConnectionData<C>) -> Self {
        ProtectedData(Arc::new((Mutex::new(connection_data), Condvar::new())))
    }

    pub fn connection_data_lock(&self) -> MutexGuard<ConnectionData<C>> {
        (self.0).0.lock().unwrap()
    }

    pub fn condvar_wait<'a>(
        &self,
        g: MutexGuard<'a, ConnectionData<C>>,
        m_timeout_ms: Option<u64>,
    ) -> (MutexGuard<'a, ConnectionData<C>>, bool) {
        match m_timeout_ms {
            Some(timeout_ms) => {
                let timeout = Duration::from_millis(timeout_ms);
                let wait_result = (self.0).1.wait_timeout(g, timeout).unwrap();
                (wait_result.0, wait_result.1.timed_out())
            }
            None => ((self.0).1.wait(g).unwrap(), false),
        }
    }

    pub fn condvar_notify(&self) {
        (self.0).1.notify_one()
    }
}

impl<C> Clone for ProtectedData<C>
where
    C: Connection,
{
    fn clone(&self) -> ProtectedData<C> {
        ProtectedData(Arc::clone(&self.0))
    }
}

impl<C> Into<Arc<(Mutex<ConnectionData<C>>, Condvar)>> for ProtectedData<C>
where
    C: Connection,
{
    fn into(self) -> Arc<(Mutex<ConnectionData<C>>, Condvar)> {
        self.0
    }
}

// Internal data type used for inter-thread communications regarding connection
// pool rebalancing.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RebalanceCheck(Arc<(Mutex<bool>, Condvar)>);

impl RebalanceCheck {
    #![allow(clippy::mutex_atomic)]
    pub fn new() -> Self {
        RebalanceCheck(Arc::new((Mutex::new(false), Condvar::new())))
    }

    pub fn get_lock(&self) -> MutexGuard<bool> {
        (self.0).0.lock().unwrap()
    }

    pub fn condvar_wait<'a>(
        &self,
        g: MutexGuard<'a, bool>,
    ) -> MutexGuard<'a, bool> {
        let timeout = Duration::from_millis(500);
        let wait_result = (self.0).1.wait_timeout(g, timeout).unwrap();
        wait_result.0
    }

    pub fn condvar_notify(&self) {
        (self.0).1.notify_one()
    }
}

impl Clone for RebalanceCheck {
    fn clone(&self) -> RebalanceCheck {
        RebalanceCheck(Arc::clone(&self.0))
    }
}

impl Into<Arc<(Mutex<bool>, Condvar)>> for RebalanceCheck {
    fn into(self) -> Arc<(Mutex<bool>, Condvar)> {
        self.0
    }
}

/// Sum type representing the current state of the connection pool. Possible
/// states are running, stopping, or stopped.
#[derive(Copy, Clone, Debug)]
pub enum ConnectionPoolState {
    /// The pool is running and able to service connection claim requests.
    Running,
    /// The connection pool is performing cleanup and is no longer accepting
    /// connection claim requests.
    Stopping,
    /// The connection pool is stopped and is no longer accepting connection
    /// claim requests.
    Stopped,
}

impl fmt::Display for ConnectionPoolState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionPoolState::Running => String::from("running").fmt(fmt),
            ConnectionPoolState::Stopping => String::from("stopping").fmt(fmt),
            ConnectionPoolState::Stopped => String::from("stopped").fmt(fmt),
        }
    }
}

/// A trait that provides utility methods for shuffling a collection.
pub trait ShuffleCollection {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn swap(&mut self, i: usize, j: usize);
}

impl<T> ShuffleCollection for VecDeque<T> {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn swap(&mut self, i: usize, j: usize) {
        self.swap(i, j)
    }
}
