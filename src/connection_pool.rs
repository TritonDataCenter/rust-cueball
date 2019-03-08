/*
 * Copyright 2019 Joyent, Inc.
 */

pub mod types;

use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::{Arc, Barrier};
use std::sync::mpsc::{channel, Receiver};
use std::thread;

use slog::{Logger, warn, error};

use crate::connection::Connection;
use crate::connection_pool::types::{ConnectionData,
                                    ConnectionKeyPair,
                                    ConnectionPoolOptions,
                                    ConnectionPoolStats,
                                    ProtectedData};
use crate::error::Error;
use crate::resolver::{BackendAddedMsg,
                      BackendMsg,
                      BackendRemovedMsg,
                      Resolver};

// General TODOs
// * Incorporate pending_connections
// * More logging
// * Consider removing idle_connections stat and use connections queue size
//   instead for less overhead and reduced chance of accounting error.
// * Connection rebalancing and decoherence

#[derive(Debug)]
pub struct ConnectionPool<C, R> {
    protected_data: ProtectedData<C>,
    last_error: Option<Error>,
    resolver_thread: Option<thread::JoinHandle<()>>,
    resolver_rx_thread: Option<thread::JoinHandle<()>>,
    spares: u32,
    maximum: u32,
    claim_timeout: Option<u64>,
    log: Logger,
    _resolver: PhantomData<R>
}

impl<C, R> Clone for ConnectionPool<C, R>
where
    C: Connection,
    R: Resolver
{
    fn clone(&self) -> ConnectionPool<C, R> {
        ConnectionPool {
            protected_data: self.protected_data.clone(),
            last_error: None,
            resolver_thread: None,
            resolver_rx_thread: None,
            spares: self.spares.clone(),
            maximum: self.maximum.clone(),
            claim_timeout: self.claim_timeout.clone(),
            log: self.log.clone(),
            _resolver: PhantomData
        }
    }
}

impl<C, R> ConnectionPool<C, R>
where
    C: Connection,
    R: Resolver
{
    pub fn new(cpo: ConnectionPoolOptions<R>) -> Self {
        let connection_data = ConnectionData::new(cpo.maximum as usize);

        // Create a channel to receive notifications from the resolver
        let (tx, rx) = channel();

        let barrier = Arc::new(Barrier::new(2));

        let mut resolver = cpo.resolver;

        // Spawn a thread to run the resolver
        let barrier_clone = barrier.clone();
        let resolver_thread = thread::spawn(move || {
            // Wait until ConnectonPool is created
            barrier_clone.wait();

            resolver.start(tx);
        });

        let protected_data = ProtectedData::new(connection_data);

        // Spawn another thread to receive notifications from resolver and take
        // action
        let protected_data_clone = protected_data.clone();
        let max_connections = cpo.maximum.clone();
        let log_clone = cpo.log.clone();
        let resolver_rx_thread =
            thread::spawn(move || resolver_recv_loop::<C>(rx, max_connections, protected_data_clone, log_clone));

        let pool = ConnectionPool {
            protected_data: protected_data,
            last_error: None,
            resolver_thread: Some(resolver_thread),
            resolver_rx_thread: Some(resolver_rx_thread),
            spares: cpo.spares,
            maximum: cpo.maximum,
            claim_timeout: cpo.claim_timeout,
            log: cpo.log,
            _resolver: PhantomData
        };

        barrier.clone().wait();
        pool
    }

    pub fn stop(&self) -> () {
        std::unimplemented!()
    }

    pub fn claim(&self) -> Result<PoolConnection<C, R>, Error> {
        let mut connection_data_guard = self.protected_data.connection_data_lock();
        let mut connection_data = connection_data_guard.deref_mut();
        let mut waiting_for_connection = true;
        let mut result = Err(Error::CueballError(String::from("dummy error")));

        while waiting_for_connection {
            if connection_data.stats.idle_connections > 0 {
                match connection_data.connections.pop_front() {
                    Some(ConnectionKeyPair((key, Some(conn)))) => {
                        connection_data.stats.idle_connections =
                            connection_data.stats.idle_connections - 1;
                        waiting_for_connection = false;
                        result =
                            Ok(PoolConnection {
                                connection_pool: self.clone(),
                                connection_pair: ConnectionKeyPair((key, Some(conn)))
                            });
                    },
                    Some(ConnectionKeyPair((_key, None))) => {
                        // Should never happen
                        let err_msg = String::from("Found backend key with no connection");
                        warn!(self.log, "{}", err_msg);
                        result = Err(Error::CueballError(err_msg));
                    },
                    None => {
                        let err_msg = String::from("Unable to retrieve a connection");
                        result = Err(Error::CueballError(err_msg));
                    }
                }
            } else{
                let wait_result =
                    self.protected_data.condvar_wait(connection_data_guard,
                                                     self.claim_timeout);
                connection_data_guard = wait_result.0;
                connection_data = connection_data_guard.deref_mut();

                if wait_result.1 {
                    let err_msg = String::from("Unable to retrieve a \
                                                connection within the \
                                                claim timeout");
                    result = Err(Error::CueballError(err_msg));
                    waiting_for_connection = false;
                }
            }
        }

        return result;
    }

    pub fn try_claim(&self) -> Option<PoolConnection<C, R>> {
        let mut connection_data = self.protected_data.connection_data_lock();

        if connection_data.stats.idle_connections > 0 {
            match connection_data.connections.pop_front() {
                Some(ConnectionKeyPair((key, Some(conn)))) => {
                    connection_data.stats.idle_connections =
                        connection_data.stats.idle_connections - 1;
                    Some(PoolConnection {
                        connection_pool: self.clone(),
                        connection_pair: ConnectionKeyPair((key, Some(conn)))
                    })
                },
                Some(ConnectionKeyPair((_key, None))) => {
                    // Should never happen
                    let err_msg = String::from("Found backend key with no connection");
                    warn!(self.log, "{}", err_msg);
                    None
                },
                None => {
                    None
                }
            }
        } else {
            None
        }
    }

    pub fn get_last_error(&self) -> Option<String> {
        std::unimplemented!()
    }

    pub fn get_stats(&self) -> Option<ConnectionPoolStats> {
        std::unimplemented!()
    }

    fn replace(&self, connection_key_pair: ConnectionKeyPair<C>)
    where
        C: Connection
    {
        let mut connection_data = self.protected_data.connection_data_lock();
        let (key, m_conn) = connection_key_pair.into();

        if connection_data.dead_connection_keys.contains(&key) {
            // Connection was removed by resolver so close it and remove from
            // dead_connection_keys set
            match m_conn {
                Some(mut conn) => {
                    log_error(&self.log, conn.close())
                },
                None => {
                    // Should never get here
                    let err_msg = String::from("Found backend key with no connection");
                    warn!(self.log, "{}", err_msg);
                    ()
                }
            }

            let _ = connection_data.dead_connection_keys.remove(&key);
        } else {
            connection_data.connections.push_back((key, m_conn).into());
            connection_data.stats.idle_connections =
                connection_data.stats.idle_connections + 1;
            self.protected_data.condvar_notify();
        }
    }
}

#[derive(Debug)]
pub struct PoolConnection<C, R>
where
    C: Connection,
    R: Resolver
{
    connection_pool: ConnectionPool<C, R>,
    connection_pair: ConnectionKeyPair<C>
}

impl<C, R> Drop for PoolConnection<C, R>
where
    C: Connection,
    R: Resolver
{
    fn drop(&mut self) {
        let ConnectionKeyPair((key, m_conn)) = &mut self.connection_pair;
        match m_conn.take() {
            Some(conn) => {
                self.connection_pool.replace((key.clone(), Some(conn)).into());
            },
            None => {
                // If we arrive here then the connection is no longer available
                // and cannot be returned to the pool
                // TODO: Log this case
            }
        }
    }
}


fn log_error(log: &Logger, result: Result<(), Error>) {
    if let Err(err) = result {
        let err_str = format!("{}", err);
        error!(log, "{}", err_str);
        ()
    }
}

fn add_backend<C>(msg: BackendAddedMsg,
                     max_connections: &u32,
                     protected_data: ProtectedData<C>)
                     -> Result<(), Error>
where
    C: Connection
{
    // Check if we already have enough connections
    let mut connection_data = protected_data.connection_data_lock();

    // TODO: Account for spares
    if connection_data.stats.total_connections < *max_connections {
        // Try to establish connection
        let mut conn = C::new(&msg.backend);
        conn.connect().and_then(|_| {
            // Update connection info and stats
            let connection_key_pair =(msg.key.clone(), Some(conn)).into();
            connection_data.connections.push_back(connection_key_pair);
            connection_data.stats.idle_connections =
                connection_data.stats.idle_connections + 1;
            connection_data.stats.total_connections =
                connection_data.stats.total_connections + 1;

            protected_data.condvar_notify();

            Ok(())
        })
    } else {
        let err_msg = String::from("Maximum connection count already reached");
        Err(Error::CueballError(err_msg))
    }
}


fn remove_backend<C>(msg: BackendRemovedMsg,
                     protected_data: ProtectedData<C>,
                     log: &Logger)
                     -> Result<(), Error>
where
    C: Connection
{
    let mut connection_data = protected_data.connection_data_lock();

    connection_data.stats.total_connections =
        connection_data.stats.total_connections - 1;

    // Find the index of the connection key pair. Use a fake connection key pair
    // that only contains the key of interest. The custom Eq and PartialEq
    // instances dictate that equality is solely determined by the first member
    // of the pair.
    let fake_connection_key_pair = (msg.0.clone(), None).into();
    let m_pos = connection_data
        .connections
        .iter()
        .position(|x| *x == fake_connection_key_pair);

    match m_pos {
        Some(pos) => {
            // Remove the connection from the connections queue and close it
            let remove_result =
                connection_data.connections.remove(pos)
                .and_then(|ConnectionKeyPair((_key, m_conn))| m_conn)
                .ok_or_else(|| {
                    // Should never happen
                    let err_msg = String::from("Found backend key with no connection");
                    warn!(log, "{}", err_msg);
                    Error::CueballError(err_msg)
                })
                .and_then(|mut conn| conn.close());

            log_error(&log, remove_result);

            // Update idle connections count
            connection_data.stats.idle_connections =
                connection_data.stats.idle_connections - 1;
        },
        None => {
            // Connection is in use so add to dead connections queue
            connection_data.dead_connection_keys.insert(msg.0.clone());
        }
    }
    Ok(())
}

fn resolver_recv_loop<C>(rx: Receiver<BackendMsg>,
                         max_connections: u32,
                         protected_data: ProtectedData<C>,
                         log: Logger)
where
    C: Connection
{
    let mut done = false;
    while !done {
        let result =
            match rx.recv() {
                Ok(BackendMsg::AddedMsg(added_msg)) =>
                    add_backend::<C>(added_msg, &max_connections, protected_data.clone()),
                Ok(BackendMsg::RemovedMsg(removed_msg)) =>
                    remove_backend::<C>(removed_msg, protected_data.clone(), &log),
                Err(_recv_err) => {
                    done = true;
                    Ok(())
                }
            };
        log_error(&log, result);
    }
}
