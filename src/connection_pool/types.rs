/*
 * Copyright 2019 Joyent, Inc.
 */

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use slog::Logger;

use crate::backend::BackendKey;
use crate::connection::Connection;


#[derive(Copy, Clone, Debug)]
pub struct ConnectionPoolStats {
    pub total_connections: u32,
    pub idle_connections: u32,
    pub pending_connections: u32
}

impl ConnectionPoolStats {
    pub fn new() -> Self {
        ConnectionPoolStats {
            total_connections: 0,
            idle_connections: 0,
            pending_connections: 0
        }
    }
}

#[derive(Debug)]
pub struct ConnectionPoolOptions<R> {
    pub domain: String,
    pub spares: u32,
    pub maximum: u32,
    pub service: Option<String>,
    pub claim_timeout: Option<u64>,
    pub resolver: R,
    pub log: Logger
}

#[derive(Clone, Debug)]
pub struct ConnectionKeyPair<C>(pub (BackendKey, Option<C>));

impl<C> PartialEq for ConnectionKeyPair<C>
where
    C: Connection
{
    fn eq(&self, other: &ConnectionKeyPair<C>) -> bool {
        (self.0).0 == (other.0).0
    }
}

impl<C> Eq for ConnectionKeyPair<C>
where
    C: Connection
{}

impl<C> Ord for ConnectionKeyPair<C>
where
    C: Connection
{
    fn cmp(&self, other: &ConnectionKeyPair<C>) -> Ordering {
        (self.0).0.cmp(&(other.0).0)
    }
}

impl<C> PartialOrd for ConnectionKeyPair<C>
where
    C: Connection
{
    fn partial_cmp(&self, other: &ConnectionKeyPair<C>) -> Option<Ordering> {
        (self.0).0.partial_cmp(&(other.0).0)
    }
}

impl<C> From<(BackendKey, C)> for ConnectionKeyPair<C>
where
    C: Connection
{
    fn from(pair: (BackendKey, C)) -> Self {
        ConnectionKeyPair((pair.0, Some(pair.1)))
    }
}

impl<C> From<(BackendKey, Option<C>)> for ConnectionKeyPair<C>
where
    C: Connection
{
    fn from(pair: (BackendKey, Option<C>)) -> Self {
        ConnectionKeyPair((pair.0, pair.1))
    }
}

impl<C> Into<(BackendKey, Option<C>)> for ConnectionKeyPair<C>
where
    C: Connection
{
    fn into(self) -> (BackendKey, Option<C>) {
        ((self.0).0, (self.0).1)
    }
}


#[derive(Clone, Debug)]
pub struct ConnectionData<C> {
    pub connections: VecDeque<ConnectionKeyPair<C>>,
    pub stats: ConnectionPoolStats,
    pub dead_connection_keys: HashSet<BackendKey>
}

impl<C> ConnectionData<C>
where
    C: Connection
{
    pub fn new(max_size: usize) -> Self {
        ConnectionData {
            connections: VecDeque::with_capacity(max_size),
            stats: ConnectionPoolStats::new(),
            dead_connection_keys: HashSet::with_capacity(max_size)
        }
    }
}

#[derive(Debug)]
pub struct ProtectedData<C>(Arc<(Mutex<ConnectionData<C>>, Condvar)>);

impl<C> ProtectedData<C>
where
    C: Connection
{
    pub fn new(connection_data: ConnectionData<C>) -> Self {
        ProtectedData(Arc::new((Mutex::new(connection_data), Condvar::new())))
    }

    pub fn connection_data_lock(&self) -> MutexGuard<ConnectionData<C>> {
        (self.0).0.lock().unwrap()
    }

    pub fn condvar_wait<'a>(&self, g: MutexGuard<'a, ConnectionData<C>>, m_timeout_ms: Option<u64>)
                        -> (MutexGuard<'a, ConnectionData<C>>, bool)
    {
        match m_timeout_ms {
            Some(timeout_ms) => {
                let timeout = Duration::from_millis(timeout_ms);
                let wait_result = (self.0).1.wait_timeout(g, timeout).unwrap();
                (wait_result.0, wait_result.1.timed_out())
            },
            None => ((self.0).1.wait(g).unwrap(), false)
        }
    }

    pub fn condvar_notify(&self) {
        (self.0).1.notify_one()
    }
}

impl<C> Clone for ProtectedData<C>
where
    C: Connection
{
    fn clone(&self) -> ProtectedData<C> {
        ProtectedData(Arc::clone(&self.0))
    }
}

impl<C> Into<Arc<(Mutex<ConnectionData<C>>, Condvar)>> for ProtectedData<C>
where
    C: Connection
{
    fn into(self) -> Arc<(Mutex<ConnectionData<C>>, Condvar)> {
        self.0
    }
}
