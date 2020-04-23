// Copyright 2020 Joyent, Inc.

pub mod types;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Result as FmtResult;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Barrier};
use std::{thread, time};

use chrono::Duration;
use slog::{debug, error, info, o, trace, warn, Drain, Logger};
use timer::Guard;

use crate::backend::{Backend, BackendKey};
use crate::connection::Connection;
use crate::connection_pool::types::{
    ConnectionCount, ConnectionData, ConnectionKeyPair, ConnectionPoolOptions,
    ConnectionPoolState, ConnectionPoolStats, ProtectedData, RebalanceCheck,
    ShuffleCollection,
};
use crate::error::Error;
use crate::resolver::{
    BackendAction, BackendAddedMsg, BackendMsg, BackendRemovedMsg, Resolver,
};
use backoff::{ExponentialBackoff, Operation};

// Default number of maximum pool connections
const DEFAULT_MAX_CONNECTIONS: u32 = 10;
// Rebalance delay in milliseconds
const DEFAULT_REBALANCE_ACTION_DELAY: u64 = 100;
// Decoherence interval in seconds
const DEFAULT_DECOHERENCE_INTERVAL: u64 = 300;
// Connection health check interval in seconds
const DEFAULT_CONNECTION_CHECK_INTERVAL: u64 = 30;

/// A pool of connections to a multi-node service
pub struct ConnectionPool<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send + 'static,
{
    protected_data: ProtectedData<C>,
    resolver_thread: Option<thread::JoinHandle<()>>,
    resolver_rx_thread: Option<thread::JoinHandle<()>>,
    resolver_tx: Option<Sender<BackendMsg>>,
    max_connections: u32,
    claim_timeout: Option<u64>,
    rebalance_check: RebalanceCheck,
    rebalance_thread: Option<thread::JoinHandle<()>>,
    rebalancer_stop: Arc<AtomicBool>,
    decoherence_interval: Option<u64>,
    log: Logger,
    state: ConnectionPoolState,
    decoherence_timer: Option<timer::Timer>,
    _decoherence_timer_guard: Option<Guard>,
    connection_check_timer: Option<timer::Timer>,
    _connection_check_timer_guard: Option<Guard>,
    _resolver: PhantomData<R>,
    _connection_function: PhantomData<F>,
}

impl<C: Debug, R: Debug, F: Debug> Debug for ConnectionPool<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match *self {
            ConnectionPool {
                ref protected_data,
                ref resolver_thread,
                ref resolver_rx_thread,
                ref resolver_tx,
                ref max_connections,
                ref claim_timeout,
                ref rebalance_check,
                ref rebalance_thread,
                ref rebalancer_stop,
                ref decoherence_interval,
                ref log,
                ref state,
                ref _resolver,
                ref _connection_function,
                ..
            } => {
                let mut debug_trait_builder = f.debug_struct("ConnectionPool");
                let _ = debug_trait_builder
                    .field("protected_data", &&(protected_data));
                let _ = debug_trait_builder
                    .field("resolver_thread", &&(resolver_thread));
                let _ = debug_trait_builder
                    .field("resolver_rx_thread", &&(resolver_rx_thread));
                let _ =
                    debug_trait_builder.field("resolver_tx", &&(resolver_tx));
                let _ = debug_trait_builder
                    .field("max_connections", &&(max_connections));
                let _ = debug_trait_builder
                    .field("claim_timeout", &&(claim_timeout));
                let _ = debug_trait_builder
                    .field("rebalance_check", &&(rebalance_check));
                let _ = debug_trait_builder
                    .field("rebalance_thread", &&(rebalance_thread));
                let _ = debug_trait_builder
                    .field("rebalancer_stop", &&(rebalancer_stop));
                let _ = debug_trait_builder
                    .field("decoherence_interval", &&(decoherence_interval));
                let _ = debug_trait_builder.field("log", &&(log));
                let _ = debug_trait_builder.field("state", &&(state));
                let _ = debug_trait_builder.field("_resolver", &&(_resolver));
                let _ = debug_trait_builder
                    .field("_connection_function", &&(_connection_function));
                debug_trait_builder.finish()
            }
        }
    }
}

impl<C, R, F> Clone for ConnectionPool<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send,
{
    fn clone(&self) -> ConnectionPool<C, R, F> {
        ConnectionPool {
            protected_data: self.protected_data.clone(),
            resolver_thread: None,
            resolver_rx_thread: None,
            resolver_tx: None,
            max_connections: self.max_connections,
            claim_timeout: self.claim_timeout,
            rebalance_check: self.rebalance_check.clone(),
            rebalance_thread: None,
            rebalancer_stop: self.rebalancer_stop.clone(),
            decoherence_interval: self.decoherence_interval,
            log: self.log.clone(),
            state: self.state,
            decoherence_timer: None,
            connection_check_timer: None,
            _connection_check_timer_guard: None,
            _decoherence_timer_guard: None,
            _resolver: PhantomData,
            _connection_function: PhantomData,
        }
    }
}

impl<C, R, F> ConnectionPool<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send + 'static,
{
    pub fn new(
        cpo: ConnectionPoolOptions,
        mut resolver: R,
        create_connection: F,
    ) -> Self {
        let max_connections =
            cpo.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);

        let connection_data = ConnectionData::new(max_connections as usize);

        let logger = cpo
            .log
            .unwrap_or_else(|| Logger::root(slog_stdlog::StdLog.fuse(), o!()));

        // Create a channel to receive notifications from the resolver. The
        // connection pool make a copy of the resolver_tx side of the channel
        // for the resolver to use in communicating with the connection pool and
        // also use the resolver_tx side of the channel to notify the thread
        // that receives communications from the resolver in the event of a
        // graceful pool shutdown.
        let (resolver_tx, resolver_rx) = channel();

        let barrier = Arc::new(Barrier::new(2));

        // Spawn a thread to run the resolver
        let barrier_clone = barrier.clone();
        let resolver_tx_clone = resolver_tx.clone();
        let resolver_thread = thread::spawn(move || {
            // Wait until ConnectonPool is created
            barrier_clone.wait();

            resolver.run(resolver_tx_clone);
        });

        let protected_data = ProtectedData::new(connection_data);
        let rebalancer_check = RebalanceCheck::new();

        // Spawn another thread to receive notifications from resolver and take
        // action
        let protected_data_clone = protected_data.clone();
        let rebalancer_clone = rebalancer_check.clone();

        let resolver_log_clone = logger.clone();
        let resolver_rx_thread = thread::spawn(move || {
            resolver_recv_loop::<C>(
                resolver_rx,
                protected_data_clone,
                rebalancer_clone,
                resolver_log_clone,
            )
        });

        // Spawn a thread to manage connection rebalancing
        let rebalancer_stop = Arc::new(AtomicBool::new(false));

        let protected_data_clone2 = protected_data.clone();
        let rebalancer_clone2 = rebalancer_check.clone();
        let rebalancer_log_clone = logger.clone();
        let rebalancer_stop_clone = rebalancer_stop.clone();
        let rebalancer_action_delay = cpo
            .rebalancer_action_delay
            .unwrap_or(DEFAULT_REBALANCE_ACTION_DELAY);
        let rebalance_thread = thread::spawn(move || {
            rebalancer_loop(
                max_connections,
                rebalancer_action_delay,
                protected_data_clone2,
                rebalancer_clone2,
                rebalancer_log_clone,
                rebalancer_stop_clone,
                create_connection,
            )
        });
        let decoherence_interval = cpo
            .decoherence_interval
            .unwrap_or(DEFAULT_DECOHERENCE_INTERVAL);

        let decoherence_timer = timer::Timer::new();

        let decoherence_timer_guard = start_decoherence(
            &decoherence_timer,
            decoherence_interval,
            protected_data.clone(),
            logger.clone(),
        );

        let connection_check_interval = cpo
            .connection_check_interval
            .unwrap_or(DEFAULT_CONNECTION_CHECK_INTERVAL);

        let connection_check_timer = timer::Timer::new();

        let connection_check_timer_guard = start_connection_check(
            &connection_check_timer,
            connection_check_interval,
            protected_data.clone(),
            rebalancer_check.clone(),
            logger.clone(),
        );

        let pool = ConnectionPool {
            protected_data,
            resolver_thread: Some(resolver_thread),
            resolver_rx_thread: Some(resolver_rx_thread),
            resolver_tx: Some(resolver_tx),
            max_connections,
            claim_timeout: cpo.claim_timeout,
            rebalance_check: rebalancer_check,
            rebalance_thread: Some(rebalance_thread),
            rebalancer_stop,
            decoherence_interval: Some(decoherence_interval),
            log: logger,
            state: ConnectionPoolState::Running,
            decoherence_timer: Some(decoherence_timer),
            _decoherence_timer_guard: Some(decoherence_timer_guard),
            connection_check_timer: Some(connection_check_timer),
            _connection_check_timer_guard: Some(connection_check_timer_guard),
            _resolver: PhantomData,
            _connection_function: PhantomData,
        };

        barrier.wait();
        pool
    }

    /// Stop the connection pool and resolver and close all connections in a
    /// graceful manner. This function may only be called on the original
    /// ConnectionPool instance. Thread JoinHandles may not be cloned and
    /// therefore invocation of this function by a clone of the pool results in
    /// an error. This function will block the caller until all claimed threads
    /// are returned to the pool and are closed and until all worker threads
    /// except for the thread running the resolver have exited.
    pub fn stop(&mut self) -> Result<(), Error> {
        // Make sure this is the original ConnectionPool instance and that it is
        // safe to unwrap all the Option types needed to stop the pool.
        if self.resolver_thread.is_some()
            && self.resolver_rx_thread.is_some()
            && self.resolver_tx.is_some()
            && self.rebalance_thread.is_some()
        {
            trace!(self.log, "stop called by original pool");

            // Transition state to Stopping
            self.state = ConnectionPoolState::Stopping;

            // Notify the resolver, resolver_recv, and rebalancer threads to
            // shutdown.  Join on the thread handles for the resolver receiver
            // and rebalancer threads. If the resolver is well-behaved, dropping
            // all clones of the receiver channel should cause the resolver to
            // shut down. Do not join on the resolver thread because the code is
            // not controlled by the pool and may not properly respond to the
            // resolver receiver thread stopping. Just drop the JoinHandle for
            // the resolver thread and move along.
            self.rebalancer_stop.store(true, AtomicOrdering::Relaxed);
            let resolver_tx = self.resolver_tx.take().unwrap();
            match resolver_tx.send(BackendMsg::StopMsg) {
                Ok(()) => {
                    let resolver_rx_thread =
                        self.resolver_rx_thread.take().unwrap();
                    let _ = resolver_rx_thread.join();
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "Failed to send stop message to resolver \
                         receiver thread: {}",
                        e
                    );
                }
            }
            drop(resolver_tx);

            let rebalance_thread = self.rebalance_thread.take().unwrap();
            let _ = rebalance_thread.join();
            drop(self.resolver_thread.take().unwrap());
            trace!(self.log, "stop: joined connection pool worker threads");

            // Mark all connections as unwanted and close what is in the pool
            let mut connection_data =
                self.protected_data.connection_data_lock();

            let backends = connection_data.backends.clone();

            // Iterate through the removed backends and remove their values from the
            // connection distribution while also adding them to the
            // unwanted_connection_counts map.
            backends.iter().for_each(|b| {
                connection_data
                    .connection_distribution
                    .remove(b.0)
                    .and_then(|count| {
                        connection_data
                            .unwanted_connection_counts
                            .entry(b.0.clone())
                            .and_modify(|e| *e += count)
                            .or_insert(count);
                        Some(1)
                    });
            });

            connection_data.backends.clear();

            drop(connection_data);

            if self.decoherence_timer.is_some() {
                let _timer = self.decoherence_timer.take();
            }

            if self.connection_check_timer.is_some() {
                let _timer = self.connection_check_timer.take();
            }

            let mut connection_data =
                self.protected_data.connection_data_lock();

            info!(
                self.log,
                "connections queue size: {}",
                connection_data.connections.len()
            );

            while !connection_data.connections.is_empty() {
                match connection_data.connections.pop_front() {
                    Some(ConnectionKeyPair((key, Some(conn)))) => {
                        info!(self.log, "closing connection {}", key);
                        let close_log = self.log.clone();
                        let close_key = key.clone();
                        // Do not want to block the pool on external code so
                        // spawn threads to run the connection close
                        // function. This means the pool may move to the
                        // Stopped state prior to all connections being
                        // closed. If the program is exiting this should not
                        // matter and if the program is continuing it is the
                        // case that when this function returns a close
                        // thread has been created for all connection pool
                        // connections. In the future we might add an option
                        // to wait for all threads if that was an important
                        // use case for users.
                        let _close_thread = thread::spawn(|| {
                            close_connection(close_log, close_key, conn)
                        });
                        connection_data.stats.idle_connections -= 1.into();
                    }
                    Some(ConnectionKeyPair((_key, None))) => {
                        // Should never happen
                        let err_msg = String::from(
                            "Found backend key with no connection",
                        );
                        warn!(self.log, "{}", err_msg);
                    }
                    None => {
                        // This also should never happen here because we checked
                        // if the queue was empty before entering the loop
                        let err_msg =
                            String::from("Unable to retrieve a connection");
                        warn!(self.log, "{}", err_msg);
                    }
                }
            }

            connection_data.stats.total_connections = 0.into();

            // Move state to Stopped
            self.state = ConnectionPoolState::Stopped;
            Ok(())
        } else {
            trace!(self.log, "stop called by pool clone");
            Err(Error::StopCalledByClone)
        }
    }

    pub fn claim(&self) -> Result<PoolConnection<C, R, F>, Error> {
        let mut connection_data_guard =
            self.protected_data.connection_data_lock();
        let mut connection_data = connection_data_guard.deref_mut();
        let mut waiting_for_connection = true;
        let mut result = Err(Error::DummyError);

        let mut unwanted_connection_counts: HashMap<
            BackendKey,
            ConnectionCount,
        > = connection_data.unwanted_connection_counts.drain().collect();

        while waiting_for_connection {
            if connection_data.stats.idle_connections > 0.into() {
                match connection_data.connections.pop_front() {
                    Some(ConnectionKeyPair((key, Some(conn)))) => {
                        if unwanted_connection_counts.contains_key(&key) {
                            // This connection is unwanted so close it and try
                            // to claim the next one in the queue. Spawn a
                            // separate thread to close this connection. The
                            // implementation of this function is outside the
                            // control of the pool so isolate the execution in
                            // its own thread to be safe.
                            let close_log = self.log.clone();
                            let close_key = key.clone();
                            let _close_thread = thread::spawn(|| {
                                close_connection(close_log, close_key, conn)
                            });

                            connection_data.stats.idle_connections -= 1.into();
                            unwanted_connection_counts
                                .entry(key.clone())
                                .and_modify(|e| *e -= 1u32.into());
                            if let Some(updated_count) =
                                unwanted_connection_counts.get(&key)
                            {
                                info!(
                                    self.log,
                                    "Updated unwanted count for backend {}: {}",
                                    &key,
                                    updated_count
                                );
                                if *updated_count <= 0u32.into() {
                                    unwanted_connection_counts.remove(&key);
                                }
                            }
                        } else {
                            info!(
                                self.log,
                                "Found idle connection for backend {}", &key
                            );
                            connection_data.stats.idle_connections -= 1.into();
                            waiting_for_connection = false;
                            result = Ok(PoolConnection {
                                connection_pool: self.clone(),
                                connection_pair: ConnectionKeyPair((
                                    key,
                                    Some(conn),
                                )),
                            });
                        }
                    }
                    Some(ConnectionKeyPair((_key, None))) => {
                        // Should never happen
                        let err_msg = "Found backend key with no connection";
                        warn!(self.log, "{}", err_msg);
                        result = Err(Error::BackendWithNoConnection);
                    }
                    None => {
                        // This also should never happen here because we checked
                        // if the queue was empty before entering the loop
                        result = Err(Error::ConnectionRetrievalFailure);
                    }
                }
            } else {
                let wait_result = self
                    .protected_data
                    .condvar_wait(connection_data_guard, self.claim_timeout);
                connection_data_guard = wait_result.0;
                connection_data = connection_data_guard.deref_mut();

                if wait_result.1 {
                    result = Err(Error::ClaimFailure);
                    waiting_for_connection = false;
                }
            }
        }

        connection_data.unwanted_connection_counts = unwanted_connection_counts;

        result
    }

    pub fn try_claim(&self) -> Option<PoolConnection<C, R, F>> {
        let mut connection_data_guard =
            self.protected_data.connection_data_lock();
        let mut connection_data = connection_data_guard.deref_mut();
        let mut waiting_for_connection = true;
        let mut result: Option<PoolConnection<C, R, F>> = None;

        let mut unwanted_connection_counts: HashMap<
            BackendKey,
            ConnectionCount,
        > = connection_data.unwanted_connection_counts.drain().collect();

        while waiting_for_connection {
            if connection_data.stats.idle_connections > 0.into() {
                match connection_data.connections.pop_front() {
                    Some(ConnectionKeyPair((key, Some(conn)))) => {
                        if unwanted_connection_counts.contains_key(&key) {
                            // This connection is unwanted so close it and try
                            // to claim the next one in the queue. Spawn a
                            // separate thread to close this connection. The
                            // implementation of this function is outside the
                            // control of the pool so isolate the execution in
                            // its own thread to be safe.
                            let close_log = self.log.clone();
                            let close_key = key.clone();
                            let _close_thread = thread::spawn(|| {
                                close_connection(close_log, close_key, conn)
                            });

                            connection_data.stats.idle_connections -= 1.into();

                            unwanted_connection_counts
                                .entry(key.clone())
                                .and_modify(|e| *e -= 1u32.into());
                            if let Some(updated_count) =
                                unwanted_connection_counts.get(&key)
                            {
                                info!(
                                    self.log,
                                    "Updated unwanted count for backend {}: {}",
                                    &key,
                                    updated_count
                                );
                                if *updated_count <= 0u32.into() {
                                    unwanted_connection_counts.remove(&key);
                                }
                            }
                        } else {
                            info!(
                                self.log,
                                "Found idle connection for backend {}", &key
                            );

                            connection_data.stats.idle_connections -= 1.into();
                            waiting_for_connection = false;
                            result = Some(PoolConnection {
                                connection_pool: self.clone(),
                                connection_pair: ConnectionKeyPair((
                                    key,
                                    Some(conn),
                                )),
                            });
                        }
                    }
                    Some(ConnectionKeyPair((_key, None))) => {
                        // Should never happen
                        let err_msg = String::from(
                            "Found backend key with no connection",
                        );
                        warn!(self.log, "{}", err_msg);
                        result = None;
                    }
                    None => {
                        let _err_msg =
                            String::from("Unable to retrieve a connection");
                        result = None;
                    }
                }
            } else {
                waiting_for_connection = false;
                result = None;
            }
        }

        connection_data.unwanted_connection_counts = unwanted_connection_counts;

        result
    }

    pub fn get_stats(&self) -> Option<ConnectionPoolStats> {
        match self.state {
            ConnectionPoolState::Running => {
                let connection_data =
                    self.protected_data.connection_data_lock();
                Some(connection_data.stats)
            }
            _ => None,
        }
    }

    pub fn get_state(&self) -> String {
        self.state.to_string()
    }

    fn replace(&self, connection_key_pair: ConnectionKeyPair<C>)
    where
        C: Connection,
    {
        let mut connection_data = self.protected_data.connection_data_lock();
        let (key, m_conn) = connection_key_pair.into();
        match m_conn {
            Some(conn) => {
                if conn.has_broken() {
                    warn!(self.log, "Found an invalid connection, not returning to the pool");
                    connection_data.stats.total_connections -= 1.into();
                } else {
                    connection_data.connections.push_back((key, conn).into());
                    connection_data.stats.idle_connections += 1.into();
                }
            }
            None => warn!(self.log, "Connection not found"),
        }
        self.protected_data.condvar_notify();
    }
}

impl<C, R, F> Drop for ConnectionPool<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send,
{
    fn drop(&mut self) {
        // Stop the pool and ignore the result. The returned Result will be an
        // Err if the pool instance going out of scope is a clone,
        // but there is not further error handling to be done here.
        let _ = self.stop();
    }
}

/// A connection abstraction reprsenting a member of the pool
#[derive(Debug, Clone)]
pub struct PoolConnection<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send + 'static,
{
    connection_pool: ConnectionPool<C, R, F>,
    connection_pair: ConnectionKeyPair<C>,
}

impl<C, R, F> Drop for PoolConnection<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send,
{
    fn drop(&mut self) {
        let ConnectionKeyPair((key, m_conn)) = &mut self.connection_pair;
        match m_conn.take() {
            Some(conn) => {
                self.connection_pool
                    .replace((key.clone(), Some(conn)).into());
            }
            None => {
                // If we arrive here then the connection is no longer available
                // and cannot be returned to the pool
                warn!(
                    self.connection_pool.log,
                    "Connection for backend {} is \
                     no longer available. Cannot \
                     return to pool.",
                    &key
                );
            }
        }
    }
}

impl<C, R, F> Deref for PoolConnection<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send,
{
    type Target = C;

    fn deref(&self) -> &C {
        &(self.connection_pair.0).1.as_ref().unwrap()
    }
}

impl<C, R, F> DerefMut for PoolConnection<C, R, F>
where
    C: Connection,
    R: Resolver,
    F: FnMut(&Backend) -> C + Send,
{
    fn deref_mut(&mut self) -> &mut C {
        (self.connection_pair.0).1.as_mut().unwrap()
    }
}

fn close_connection<C>(log: Logger, key: BackendKey, mut conn: C)
where
    C: Connection,
{
    info!(log, "Closing unwanted connection for backend {}", &key);
    if let Err(err) = conn.close() {
        warn!(
            log,
            "Failed to properly close \
             unwanted connection for \
             backend {}. Reason: {}",
            &key,
            err
        );
    }
}

fn add_backend<C>(
    msg: BackendAddedMsg,
    protected_data: ProtectedData<C>,
) -> Option<BackendAction>
where
    C: Connection,
{
    let mut connection_data = protected_data.connection_data_lock();

    if !connection_data.backends.contains_key(&msg.key) {
        connection_data
            .backends
            .insert(msg.key.clone(), msg.backend);

        Some(BackendAction::BackendAdded)
    } else {
        None
    }
}

fn remove_backend<C>(
    msg: BackendRemovedMsg,
    protected_data: ProtectedData<C>,
    log: &Logger,
) -> Option<BackendAction>
where
    C: Connection,
{
    let mut connection_data = protected_data.connection_data_lock();

    if connection_data.backends.contains_key(&msg.0) {
        debug!(log, "Removing backend with key {}", &msg.0);
        connection_data.backends.remove(&msg.0);
        Some(BackendAction::BackendRemoved)
    } else {
        None
    }
}

fn rebalance_connections<C>(
    max_connections: u32,
    log: &Logger,
    protected_data: ProtectedData<C>,
) -> Result<Option<HashMap<BackendKey, ConnectionCount>>, Error>
where
    C: Connection,
{
    let mut connection_data = protected_data.connection_data_lock();
    debug!(
        log,
        "Running rebalancer on {} connections...",
        connection_data.connections.len()
    );

    let backend_count = connection_data.backends.len();
    info!(log, "Backend count: {}", &backend_count);
    if backend_count == 0 {
        warn!(log, "Not running rebalance, no backends.");
        return Ok(None);
    }

    // Calculate a new connection distribution over the set of available
    // backends and determine what additional connections need to be created and
    // what connections can be deemed unwanted.
    let mut removed_backends = Vec::with_capacity(max_connections as usize);
    connection_data
        .connection_distribution
        .iter()
        .for_each(|(k, _)| {
            if !connection_data.backends.contains_key(k) {
                removed_backends.push(k.clone());
            }
        });

    // Iterate through the removed backends and remove their values from the
    // connection distribution while also adding them to the
    // unwanted_connection_counts map.
    removed_backends.iter().for_each(|b| {
        connection_data
            .connection_distribution
            .remove(b)
            .and_then(|count| {
                connection_data
                    .unwanted_connection_counts
                    .entry(b.clone())
                    .and_modify(|e| *e += count)
                    .or_insert(count);
                Some(1)
            });
    });

    // Calculate the new connection distribution counts for the available
    // backends
    let connections_per_backend = max_connections as usize / backend_count;
    let mut connections_per_backend_rem =
        max_connections as usize % backend_count;

    // Traverse the available backends and assign each backend the value of
    // connections_per_backend in the distribution + 1 extra if
    // connections_per_backend_rem is greater than zero. Decrement
    // connections_per_backend_rem with each iteration until it reaches zero.
    // Also determine if the new value represents an addition or removal of a
    // connection for this backend and either add an entry to the added hashmap
    // or the unwanted connections map.
    let mut added_connection_counts = HashMap::with_capacity(backend_count);
    let mut connection_distribution: HashMap<BackendKey, ConnectionCount> =
        connection_data.connection_distribution.drain().collect();
    let mut unwanted_connection_counts: HashMap<BackendKey, ConnectionCount> =
        connection_data.unwanted_connection_counts.drain().collect();
    let mut pending_connections: ConnectionCount = 0.into();

    connection_data.backends.keys().for_each(|b| {
        info!(log, "Backend key: {}", b);
        let new_connection_count: ConnectionCount =
            ConnectionCount::from(if connections_per_backend_rem > 0 {
                connections_per_backend_rem -= 1;
                connections_per_backend as u32 + 1
            } else {
                connections_per_backend as u32
            });
        let old_connection_count = connection_distribution
            .get(b)
            .copied()
            .unwrap_or_else(|| ConnectionCount::from(0));

        debug!(
            log,
            "New connection count: {} Old Connection Count: {}",
            new_connection_count,
            old_connection_count
        );

        match new_connection_count.cmp(&old_connection_count) {
            Ordering::Greater => {
                let connection_delta =
                    new_connection_count - old_connection_count;
                pending_connections += connection_delta;
                added_connection_counts.insert(b.clone(), connection_delta);
                connection_distribution
                    .entry(b.clone())
                    .and_modify(|e| *e += connection_delta)
                    .or_insert(connection_delta);
            }
            Ordering::Less => {
                let connection_delta =
                    old_connection_count - new_connection_count;
                unwanted_connection_counts
                    .entry(b.clone())
                    .and_modify(|e| *e = connection_delta)
                    .or_insert(connection_delta);
            }
            Ordering::Equal => (),
        }
    });

    connection_data.connection_distribution = connection_distribution;
    connection_data.unwanted_connection_counts = unwanted_connection_counts;
    connection_data.stats.pending_connections += pending_connections;

    if !added_connection_counts.is_empty() {
        Ok(Some(added_connection_counts))
    } else {
        Ok(None)
    }
}

fn add_connections<C, F>(
    connection_counts: HashMap<BackendKey, ConnectionCount>,
    max_connections: u32,
    log: &Logger,
    protected_data: ProtectedData<C>,
    create_connection: &mut F,
) where
    C: Connection,
    F: FnMut(&Backend) -> C,
{
    connection_counts.iter().for_each(|(b_key, b_count)| {
        for _ in 0..b_count.clone().into() {
            let mut connection_data = protected_data.connection_data_lock();

            // TODO: Maybe track total unwanted connection count in stats so
            // here we can more cheaply check if total_connections -
            // unwanted_connections < max_connections
            let mut unwanted_connections_total = ConnectionCount::from(0);
            connection_data
                .unwanted_connection_counts
                .values()
                .for_each(|ucc| {
                    unwanted_connections_total += *ucc;
                });

            debug!(
                log,
                "Unwanted connection count: {}", unwanted_connections_total
            );

            let net_total_connections = connection_data.stats.total_connections
                - unwanted_connections_total;

            debug!(log, "Net total connections: {}", net_total_connections);

            if net_total_connections < max_connections.into() {
                // Try to establish connection
                debug!(
                    log,
                    "Trying to add more connections: {}", net_total_connections
                );
                let m_backend = connection_data.backends.get(b_key);
                if let Some(backend) = m_backend {
                    let mut conn = create_connection(backend);
                    let mut backoff = ExponentialBackoff::default();
                    let mut op = || {
                        debug!(log, "attempting to connect with retry...");
                        conn.connect().map_err(|e| {
                            error!(
                                log,
                                "Retrying connection \
                                 : {}",
                                e
                            );
                        })?;
                        Ok(())
                    };
                    op.retry(&mut backoff)
                        .and_then(|_| {
                            // Update connection info and stats
                            let connection_key_pair =
                                (b_key.clone(), Some(conn)).into();
                            connection_data
                                .connections
                                .push_back(connection_key_pair);
                            connection_data.stats.total_connections += 1.into();
                            connection_data.stats.idle_connections += 1.into();
                            connection_data
                                .unwanted_connection_counts
                                .entry(b_key.clone())
                                .and_modify(|e| *e -= 1u32.into());
                            connection_data.stats.pending_connections -=
                                1.into();

                            info!(
                                log,
                                "Added connection for backend {}", b_key
                            );
                            protected_data.condvar_notify();
                            Ok(())
                        })
                        .unwrap_or_else(|_| {
                            error!(
                                log,
                                "Giving up trying to establish connection"
                            );
                        });
                } else {
                    error!(
                        log,
                        "No backend information available for \
                         backend key {}",
                        &b_key
                    );
                }
            } else {
                let msg =
                    String::from("Maximum connection count already reached");
                debug!(log, "{}", msg);
            }
        }
    });
}

fn resolver_recv_loop<C>(
    rx: Receiver<BackendMsg>,
    protected_data: ProtectedData<C>,
    rebalance_check: RebalanceCheck,
    log: Logger,
) where
    C: Connection,
{
    let mut done = false;

    while !done {
        let log = log.clone();
        let result = match rx.recv() {
            Ok(BackendMsg::AddedMsg(added_msg)) => {
                info!(log, "Adding backend {}", added_msg.key);
                add_backend::<C>(added_msg, protected_data.clone())
            }
            Ok(BackendMsg::RemovedMsg(removed_msg)) => {
                remove_backend::<C>(removed_msg, protected_data.clone(), &log)
            }
            Ok(BackendMsg::StopMsg) => {
                done = true;
                None
            }
            Ok(BackendMsg::HeartbeatMsg) => None,
            Err(_recv_err) => {
                done = true;
                None
            }
        };
        if result.is_some() {
            // Spawn a new thread so as not to block the resolver thread waiting
            // for a lock the rebalancer thread might hold
            let rebalance_clone = rebalance_check.clone();
            thread::spawn(move || {
                let mut rebalance = rebalance_clone.get_lock();
                if !*rebalance {
                    *rebalance = true;
                    trace!(
                        log,
                        "resolver_recv_loop notifying rebalance condvar"
                    );
                    rebalance_clone.condvar_notify();
                }
            });
        }
    }
}

fn rebalancer_loop<C, F>(
    max_connections: u32,
    rebalance_action_delay: u64,
    protected_data: ProtectedData<C>,
    rebalance_check: RebalanceCheck,
    log: Logger,
    stop: Arc<AtomicBool>,
    mut create_connection: F,
) where
    C: Connection,
    F: FnMut(&Backend) -> C,
{
    let mut done = stop.load(AtomicOrdering::Relaxed);
    while !done {
        let mut rebalance = rebalance_check.get_lock();
        trace!(log, "starting condvar wait");
        rebalance = rebalance_check.condvar_wait(rebalance);
        trace!(log, "condvar received notification");
        if *rebalance {
            // Briefly sleep in case the resolver notifies the pool of multiple
            // changes within a short period
            let sleep_time =
                time::Duration::from_millis(rebalance_action_delay);
            thread::sleep(sleep_time);

            debug!(log, "rebalance var true");

            let rebalance_result = rebalance_connections(
                max_connections,
                &log,
                protected_data.clone(),
            );

            debug!(
                log,
                "Connection rebalance completed: {:#?}", rebalance_result
            );

            if let Ok(Some(added_connection_count)) = rebalance_result {
                debug!(log, "Adding new connections");
                add_connections(
                    added_connection_count,
                    max_connections,
                    &log,
                    protected_data.clone(),
                    &mut create_connection,
                )
            }
            *rebalance = false;
        }

        done = stop.load(AtomicOrdering::Relaxed);
    }
    trace!(log, "rebalancer_loop exiting");
}

/// Start a thread to run periodic decoherence on the connection pool
fn start_decoherence<C>(
    timer: &timer::Timer,
    decoherence_interval: u64,
    protected_data: ProtectedData<C>,
    log: Logger,
) -> Guard
where
    C: Connection,
{
    debug!(
        log,
        "starting decoherence task, interval {} seconds", decoherence_interval
    );
    timer.schedule_repeating(
        Duration::seconds(decoherence_interval as i64),
        move || reshuffle_connection_queue(protected_data.clone(), log.clone()),
    )
}

fn reshuffle_connection_queue<C>(protected_data: ProtectedData<C>, log: Logger)
where
    C: Connection,
{
    debug!(log, "Performing connection decoherence shuffle...");

    let mut connection_data = protected_data.connection_data_lock();
    shuffle(
        &mut connection_data.connections,
        rand::thread_rng(),
        log.clone(),
    );
}

/// Fisher-Yates shuffle
fn shuffle<T, R>(connections: &mut T, mut rng: R, log: Logger)
where
    T: ShuffleCollection,
    R: rand::Rng,
{
    let mut i = connections.len();
    while i > 1 {
        i -= 1;
        let new_idx = rng.gen_range(0, i);
        debug!(
            log,
            "randomization puts item at idx {} to idx {}", i, new_idx
        );
        connections.swap(i, new_idx);
    }
}

/// Start a thread to run periodic health checks on the connection pool
fn start_connection_check<C>(
    timer: &timer::Timer,
    conn_check_interval: u64,
    protected_data: ProtectedData<C>,
    rebalance_check: RebalanceCheck,
    log: Logger,
) -> Guard
where
    C: Connection,
{
    debug!(
        log,
        "starting connection health task, interval {} seconds",
        conn_check_interval
    );
    timer.schedule_repeating(
        Duration::seconds(conn_check_interval as i64),
        move || {
            check_pool_connections(
                protected_data.clone(),
                rebalance_check.clone(),
                log.clone(),
            )
        },
    )
}

fn check_pool_connections<C>(
    protected_data: ProtectedData<C>,
    rebalance_check: RebalanceCheck,
    log: Logger,
) where
    C: Connection,
{
    let mut connection_data = protected_data.connection_data_lock();
    let len = connection_data.connections.len();

    if len == 0 {
        debug!(log, "No connections found, signaling rebalance check");
        let mut rebalance = rebalance_check.get_lock();
        *rebalance = true;
        trace!(log, "check_pool_connections notifying rebalance condvar");
        rebalance_check.condvar_notify();
        return;
    }

    debug!(log, "Performing connection check on {} connections", len);

    let backend_count = connection_data.backends.len();
    let mut remove_count = HashMap::with_capacity(backend_count);
    let mut removed = 0;
    connection_data.connections.retain(|pair| match pair {
        ConnectionKeyPair((key, Some(conn))) => {
            if conn.has_broken() {
                warn!(log, "found broken connection!");
                removed += 1;
                *remove_count.entry(key.clone()).or_insert(0) += 1;
                false
            } else {
                true
            }
        }
        ConnectionKeyPair((key, None)) => {
            warn!(log, "found malformed connection");
            removed += 1;
            *remove_count.entry(key.clone()).or_insert(0) += 1;
            false
        }
    });
    debug!(log, "Removed {} from connection pool", removed);

    if removed > 0 {
        for (key, count) in remove_count.iter() {
            connection_data
                .connection_distribution
                .entry(key.clone())
                .and_modify(|e| {
                    *e -= ConnectionCount::from(*count);
                    debug!(
                        log,
                        "Connection count for {} now: {}",
                        key.clone(),
                        *e
                    );
                });
        }
        connection_data.stats.total_connections -= removed.into();

        debug!(
            log,
            "idle_connections now: {}, total_connections now: {}",
            connection_data.stats.idle_connections,
            connection_data.stats.total_connections
        );

        let mut rebalance = rebalance_check.get_lock();
        if !*rebalance {
            debug!(log, "attempting to signal rebalance check");
            *rebalance = true;
            rebalance_check.condvar_notify();
        }
    }
}
