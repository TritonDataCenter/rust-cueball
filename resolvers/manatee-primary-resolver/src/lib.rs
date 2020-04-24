//
// Copyright 2020 Joyent, Inc.
//
// THEORY STATEMENT -- READ THIS FIRST!
//
// This library has just one task: watch a zookeeper node for changes and notify
// users of the library when changes occur. This is accomplished in one big pair
// of nested loops. The code structure looks a little like this:
//
// ManateePrimaryResolver::run()
//   -> Spawns tokio task
//     -> Runs outer loop, which handles connecting/reconnecting to zookeeper.
//        The logic here is in the connect_loop() function.
//        -> Runs inner loop, which handles setting/resetting the zookeeper
//           watch. The logic here is in the watch_loop() function.
//           -> For every change in zookeeper data, calls the process_value()
//              function. This function parses the new zookeeper data and
//              notifies the user of the change in data if necessary.
//
// Note that this file also contains unit tests for process_value().
//

use std::convert::From;
use std::default::Default;
use std::fmt::Debug;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use backoff::backoff::Backoff;
use backoff::default::{MAX_INTERVAL_MILLIS, MULTIPLIER};
use backoff::ExponentialBackoff;
use clap::{crate_name, crate_version};
use failure::Error as FailureError;
use futures::future::{loop_fn, ok, Either, Future, Loop};
use futures::stream::Stream;
use itertools::Itertools;
use lazy_static::lazy_static;
use rand;
use rand::Rng;
use serde::Deserialize;
use serde_json;
use serde_json::Value as SerdeJsonValue;
use slog::Result as SlogResult;
use slog::Value as SlogValue;
use slog::{
    debug, error, info, o, Drain, Key, LevelFilter, Logger, Record, Serializer,
};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::timer::timeout::Error as TimeoutError;
use tokio::timer::Delay;
use tokio_zookeeper::{
    KeeperState, WatchedEvent, WatchedEventType, ZooKeeper, ZooKeeperBuilder,
};
use url::Url;

use cueball::backend::{self, Backend, BackendAddress, BackendKey};
use cueball::resolver::{
    BackendAddedMsg, BackendMsg, BackendRemovedMsg, Resolver,
};

pub mod common;

//
// The interval at which the resolver should send heartbeats via the
// connection pool channel. Public for use in tests.
//
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

//
// Timeout for zookeeper sessions.
//
const SESSION_TIMEOUT: Duration = Duration::from_secs(5);

//
// Timeout for the initial tcp connect operation to zookeeper.
//
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

//
// To be used when we don't want to wait between loop iterations.
//
const NO_DELAY: Duration = Duration::from_secs(0);

lazy_static! {
    //
    // The maximum Duration that next_backoff() can return (when using the
    // default backoff parameters). Public for use in tests.
    //
    pub static ref MAX_BACKOFF_INTERVAL: Duration = Duration::from_millis(
        (MAX_INTERVAL_MILLIS as f64 * MULTIPLIER).ceil() as u64,
    );

    //
    // The amount of time that must elapse after successful connection without
    // an error occurring in order for the resolver state to be considered
    // stable, at which point the backoff state is reset.
    //
    // We choose the threshold based on MAX_BACKOFF_INTERVAL because if the
    // threshold were smaller, a given backoff interval could be bigger than the
    // threshold, so we would prematurely reset the backoff before the operation
    // even got a chance to try again. The threshold could be bigger, but what's
    // the point in that?
    //
    // We add a little slack so the backoff doesn't get
    // reset just before a (possibly failing) reconnect attempt is made.
    //
    static ref BACKOFF_RESET_THRESHOLD: Duration =
        *MAX_BACKOFF_INTERVAL + Duration::from_secs(1);
}

//
// An error type to be used internally.
//
#[derive(Clone, Debug, PartialEq)]
enum ResolverError {
    InvalidZkJson,
    InvalidZkData(ZkDataField),
    MissingZkData(ZkDataField),
    ConnectionPoolShutdown,
}

impl ResolverError {
    ///
    /// This function provides a means of determining whether or not a given
    /// error should cause the resolver to stop.
    ///
    fn should_stop(&self) -> bool {
        match self {
            ResolverError::ConnectionPoolShutdown => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ZkDataField {
    Ip,
    Port,
    PostgresUrl,
}

#[derive(Debug)]
pub enum ZkConnectStringError {
    EmptyString,
    MalformedAddr,
}

impl From<AddrParseError> for ZkConnectStringError {
    fn from(_: AddrParseError) -> Self {
        ZkConnectStringError::MalformedAddr
    }
}

///
/// `ZkConnectString` represents a list of zookeeper addresses to connect to.
///
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ZkConnectString(Vec<SocketAddr>);

impl ZkConnectString {
    ///
    /// Gets a reference to the SocketAddr at the provided index. Returns None
    /// if the index is out of bounds.
    ///
    fn get_addr_at(&self, index: usize) -> Option<SocketAddr> {
        self.0.get(index).cloned()
    }

    ///
    /// Returns the number of addresses in the ZkConnectString
    ///
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl ToString for ZkConnectString {
    fn to_string(&self) -> String {
        self.0
            .iter()
            .map(|x| x.to_string())
            .intersperse(String::from(","))
            .collect()
    }
}

impl FromStr for ZkConnectString {
    type Err = ZkConnectStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ZkConnectStringError::EmptyString);
        }
        let acc: Result<Vec<SocketAddr>, Self::Err> = Ok(vec![]);
        s.split(',')
            .map(|x| SocketAddr::from_str(x))
            .fold(acc, |acc, x| match (acc, x) {
                (Ok(mut addrs), Ok(addr)) => {
                    addrs.push(addr);
                    Ok(addrs)
                }
                (Err(e), _) => Err(e),
                (_, Err(e)) => Err(ZkConnectStringError::from(e)),
            })
            .and_then(|x| Ok(ZkConnectString(x)))
    }
}

//
// Encapsulates a ZkConnectString with some bookkeeping to keep track of which
// address the resolver should attempt to connect to next, and how many
// connection attempts have failed in a row. Provides methods for getting the
// next address, resetting the number of failed attempts, and checking if
// the resolver should wait before trying to connect again.
//
#[derive(Debug, Clone)]
struct ZkConnectStringState {
    conn_str: ZkConnectString,
    curr_idx: usize,
    conn_attempts: usize,
}

impl ZkConnectStringState {
    fn new(conn_str: ZkConnectString) -> Self {
        let mut rng = rand::thread_rng();
        let idx: usize = rng.gen_range(0, conn_str.len());

        ZkConnectStringState {
            conn_str,
            curr_idx: idx,
            conn_attempts: 0,
        }
    }

    fn next_addr(&mut self) -> SocketAddr {
        let ret = self
            .conn_str
            .get_addr_at(self.curr_idx)
            .expect("connect string access out of bounds");
        self.curr_idx += 1;
        self.curr_idx %= self.conn_str.len();
        self.conn_attempts += 1;
        ret
    }

    fn reset_attempts(&mut self) {
        self.conn_attempts = 0;
    }

    fn should_wait(&self) -> bool {
        self.conn_attempts == self.conn_str.len()
    }
}

///
/// A serializable type to be used in log entries. Wraps around any type that
/// implements Debug and uses the Debug representation of the type as the
/// serialized output.
///
struct LogItem<T>(T)
where
    T: Debug;

impl<T: Debug> SlogValue for LogItem<T> {
    fn serialize(
        &self,
        _rec: &Record,
        key: Key,
        serializer: &mut dyn Serializer,
    ) -> SlogResult {
        serializer.emit_str(key, &format!("{:?}", self.0))
    }
}

// Represents an action to be taken in the event of a connection error.
enum NextAction {
    //
    // The Duration field is the amount of time to wait before reconnecting.
    //
    Reconnect(Duration),
    Stop,
}

//
// Encapsulates the state that changes between iterations of watch_loop().
//
struct WatchLoopState {
    watcher: Box<dyn Stream<Item = WatchedEvent, Error = ()> + Send>,
    curr_event: WatchedEvent,
    delay: Duration,
}

//
// Encapsulates a backoff object and adds the notion of a time threshold that
// must be reached before the connection is considered stable and the backoff
// state is reset. This threshold is managed automatically using a background
// thread -- users can just call ResolverBackoff::next_backoff() normally.
//
struct ResolverBackoff {
    backoff: Arc<Mutex<ExponentialBackoff>>,
    //
    // Internal channel used for indicating that an error has occurred to the
    // stability-tracking thread
    //
    error_tx: Sender<ResolverBackoffMsg>,
    log: Logger,
}

//
// For use by ResolverBackoff object.
//
enum ResolverBackoffMsg {
    //
    // The Duration is the duration of the backoff in response to the error.
    //
    ErrorOccurred(Duration),
}

//
// For use by ResolverBackoff object.
//
enum ResolverBackoffState {
    Stable,
    //
    // The Duration is the duration of the backoff in response to the error.
    //
    Unstable(Duration),
}

impl ResolverBackoff {
    fn new(log: Logger) -> Self {
        let mut backoff = ExponentialBackoff::default();
        //
        // We'd rather the resolver not give up trying to reconnect, so we
        // set the max_elapsed_time to `None` so next_backoff() always returns
        // a valid interval.
        //
        backoff.max_elapsed_time = None;

        let (error_tx, error_rx) = channel();
        let backoff = Arc::new(Mutex::new(backoff));

        //
        // Start the stability-tracking thread.
        //
        //
        // The waiting period for stability starts from the _time of successful
        // connection_. The time of successful connection, if it occurs at all,
        // will always be equal to (time of last error + duration of resulting
        // backoff). Thus, from time of last error, we must wait (duration of
        // resulting backoff + stability threshold) in order for the connection
        // to be considered stable. Thus, we pass the backoff duration from
        // next_backoff to this thread so the thread can figure out how long to
        // wait for stability.
        //
        // Note that this thread doesn't actually _know_ if the connect
        // operation succeeds, because we only touch the ResolverBackoff
        // object when an error occurs. However, we can assume that it succeeds
        // when waiting for stability, because, if the connect operation fails,
        // this thread will receive another error and restart the wait period.
        //
        // If the connect operation takes longer than BACKOFF_RESET_THRESHOLD to
        // complete and then fails, we'll reset the backoff erroneously. This
        // situation is highly unlikely, so we'll cross that bridge when we come
        // to it.
        //
        let backoff_clone = Arc::clone(&backoff);
        let thread_log = log.clone();
        debug!(log, "spawning stability-tracking thread");
        thread::spawn(move || {
            let mut state = ResolverBackoffState::Stable;
            loop {
                match state {
                    ResolverBackoffState::Stable => {
                        //
                        // Wait for an error to happen
                        //
                        debug!(thread_log, "backoff stable; waiting for error");
                        match error_rx.recv() {
                            //
                            // * zero days since last accident *
                            //
                            Ok(ResolverBackoffMsg::ErrorOccurred(
                                new_backoff,
                            )) => {
                                info!(
                                    thread_log,
                                    "error received; backoff transitioning to \
                                     unstable state"
                                );
                                state =
                                    ResolverBackoffState::Unstable(new_backoff);
                                continue;
                            }
                            //
                            // ResolverBackoff object was dropped, so we exit
                            // the thread
                            //
                            Err(_) => break,
                        }
                    }
                    ResolverBackoffState::Unstable(current_backoff) => {
                        debug!(
                            thread_log,
                            "backoff unstable; waiting for stability"
                        );
                        //
                        // See large comment above for explanation of why we
                        // wait this long
                        //
                        match error_rx.recv_timeout(
                            current_backoff + *BACKOFF_RESET_THRESHOLD,
                        ) {
                            //
                            // We got another error, so restart the countdown
                            //
                            Ok(ResolverBackoffMsg::ErrorOccurred(
                                new_backoff,
                            )) => {
                                debug!(
                                    thread_log,
                                    "error received while waiting for \
                                     stability; restarting wait period"
                                );
                                state =
                                    ResolverBackoffState::Unstable(new_backoff);
                                continue;
                            }
                            //
                            // Timeout waiting for an error: the stability
                            // threshold has been reached, so reset the backoff
                            //
                            Err(RecvTimeoutError::Timeout) => {
                                info!(
                                    thread_log,
                                    "stability threshold reached; resetting backoff"
                                );
                                let mut backoff = backoff_clone.lock().unwrap();
                                backoff.reset();
                                state = ResolverBackoffState::Stable
                            }
                            //
                            // ResolverBackoff object was dropped, so we exit
                            // the thread
                            //
                            Err(RecvTimeoutError::Disconnected) => break,
                        }
                    }
                }
            }
            debug!(thread_log, "stability-tracking thread exiting");
        });

        ResolverBackoff {
            backoff,
            error_tx,
            log,
        }
    }

    fn next_backoff(&mut self) -> Duration {
        let mut backoff = self.backoff.lock().unwrap();
        //
        // This should never fail because we set max_elapsed_time to `None` in
        // ResolverBackoff::new().
        //
        let next_backoff = backoff.next_backoff().expect(
            "next_backoff returned
            None; max_elapsed_time has been reached erroneously",
        );

        //
        // Notify the stability-tracking thread that an error has occurred
        //
        self.error_tx
            .send(ResolverBackoffMsg::ErrorOccurred(next_backoff))
            .expect("Error sending over error_tx");

        debug!(self.log, "retrying with backoff {:?}", next_backoff);
        next_backoff
    }
}

//
// For use as argument to next_delay().
//
#[derive(Debug, Clone)]
enum DelayBehavior {
    AlwaysWait,
    CheckConnState(Arc<Mutex<ZkConnectStringState>>),
}

//
// Helper function: Accepts a ResolverBackoff and a DelayBehavior. Returns a
// duration to wait accordingly, consulting the provided ZkConnectStringState if
// applicable.
//
// NOTE: also resets the ZkConnectStringState's connection attempts if
// `behavior` is CheckConnState and it is found that we should wait.
//
fn next_delay(
    backoff: &Arc<Mutex<ResolverBackoff>>,
    behavior: &DelayBehavior,
) -> Duration {
    match behavior {
        DelayBehavior::AlwaysWait => {
            let backoff = &mut backoff.lock().unwrap();
            backoff.next_backoff()
        }
        DelayBehavior::CheckConnState(conn_str_state) => {
            let mut conn_str_state = conn_str_state.lock().unwrap();
            let should_wait = conn_str_state.should_wait();
            if should_wait {
                conn_str_state.reset_attempts();
                let backoff = &mut backoff.lock().unwrap();
                backoff.next_backoff()
            } else {
                NO_DELAY
            }
        }
    }
}

#[derive(Debug)]
pub struct ManateePrimaryResolver {
    ///
    /// The addresses of the Zookeeper cluster the Resolver is connecting to,
    /// along with associated state
    ///
    conn_str_state: Arc<Mutex<ZkConnectStringState>>,
    ///
    /// The Zookeeper path for manatee cluster state for the shard. *e.g.*
    /// "/manatee/1.moray.coal.joyent.us/state"
    ///
    cluster_state_path: String,
    ///
    /// The key representation of the last backend sent to the cueball
    /// connection pool. Persists across multiple calls to run().
    ///
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    ///
    /// Indicates whether or not the resolver is running. This is slightly
    /// superfluous (this field is `true` for exactly the duration of each
    /// call to run(), and false otherwise), but could be useful if the caller
    /// wants to check if the resolver is running for some reason.
    ///
    pub is_running: bool,
    ///
    /// The ManateePrimaryResolver's root log
    ///
    log: Logger,
}

impl ManateePrimaryResolver {
    ///
    /// Creates a new ManateePrimaryResolver instance.
    ///
    /// # Arguments
    ///
    /// * `conn_str` - a comma-separated list of the zookeeper instances
    ///   in the cluster
    /// * `path` - The path to the root node in zookeeper for the shard we're
    ///    watching
    ///
    pub fn new(
        conn_str: ZkConnectString,
        path: String,
        log: Option<Logger>,
    ) -> Self {
        let cluster_state_path = [&path, "/state"].concat();

        //
        // Add the log_values to the passed-in logger, or create a new logger if
        // the caller did not pass one in
        //
        let log = log.unwrap_or_else(|| {
            Logger::root(
                Mutex::new(LevelFilter::new(
                    slog_bunyan::with_name(crate_name!(), std::io::stdout())
                        .build(),
                    slog::Level::Info,
                ))
                .fuse(),
                o!("build-id" => crate_version!()),
            )
        });

        ManateePrimaryResolver {
            conn_str_state: Arc::new(Mutex::new(ZkConnectStringState::new(
                conn_str,
            ))),
            cluster_state_path,
            last_backend: Arc::new(Mutex::new(None)),
            is_running: false,
            log,
        }
    }
}

impl Resolver for ManateePrimaryResolver {
    //
    // The resolver object is not Sync, so we can assume that only one instance
    // of this function is running at once, because callers will have to control
    // concurrent access.
    //
    // If the connection pool closes the receiving end of the channel, this
    // function may not return right away -- this function will not notice that
    // the pool has disconnected until this function tries to send another
    // heartbeat, at which point this function will return. This means that the
    // time between disconnection and function return is at most the length of
    // HEARTBEAT_INTERVAL. Any change in the meantime will be picked up by the
    // next call to run().
    //
    // Indeed, the heartbeat messages exist solely as a time-boxed method to
    // test whether the connection pool has closed the channel, so we don't leak
    // resolver threads.
    //
    fn run(&mut self, s: Sender<BackendMsg>) {
        debug!(self.log, "run() method entered");

        let mut rt = Runtime::new().unwrap();
        //
        // There's no need to check if the pool is already running and return
        // early, because multiple instances of this function _cannot_ be
        // running concurrently -- see this function's header comment.
        //
        self.is_running = true;

        let conn_backoff = Arc::new(Mutex::new(ResolverBackoff::new(
            self.log.new(o!("component" => "conn_backoff")),
        )));
        let watch_backoff = Arc::new(Mutex::new(ResolverBackoff::new(
            self.log.new(o!("component" => "watch_backoff")),
        )));
        let loop_core = ResolverCore {
            pool_tx: s.clone(),
            last_backend: Arc::clone(&self.last_backend),
            conn_str_state: Arc::clone(&self.conn_str_state),
            cluster_state_path: self.cluster_state_path.clone(),
            conn_backoff,
            watch_backoff,
            log: self.log.clone(),
        };
        let at_log = self.log.clone();

        let exited = Arc::new(AtomicBool::new(false));
        let exited_clone = Arc::clone(&exited);

        //
        // Start the event-processing task. This is structured as two nested
        // loops: one to handle the zookeeper connection and one to handle
        // setting the watch. These are handled by the connect_loop() and
        // watch_loop() functions, respectively.
        //
        info!(self.log, "run(): starting runtime");
        rt.spawn(
            //
            // Outer loop. Handles connecting to zookeeper. A new loop iteration
            // means a new zookeeper connection. We break from the loop if we
            // discover that the user has closed the receiving channel, which
            // is their sole means of stopping the client.
            //
            // Arg: Time to wait before attempting to connect. Initially 0s.
            //     Repeated iterations of the loop set a delay before
            //     connecting.
            // Loop::Break type: ()
            //
            loop_fn(NO_DELAY, move |delay| {
                loop_core.clone().connect_loop(delay)
            })
            .and_then(move |_| {
                info!(at_log, "Event-processing task stopping");
                exited_clone.store(true, Ordering::Relaxed);
                Ok(())
            })
            .map(|_| ())
            .map_err(|_| {
                unreachable!("connect_loop() should never return an error")
            }),
        );

        //
        // Heartbeat-sending loop. If we break from this loop, the resolver
        // exits.
        //
        loop {
            if exited.load(Ordering::Relaxed) {
                info!(
                    self.log,
                    "event-processing task exited; stopping heartbeats"
                );
                break;
            }
            if s.send(BackendMsg::HeartbeatMsg).is_err() {
                info!(self.log, "Connection pool channel closed");
                break;
            }
            thread::sleep(HEARTBEAT_INTERVAL);
        }

        //
        // We shut down the background watch-looping thread. It may have already
        // exited by itself if it noticed that the connection pool closed its
        // channel, but there's no harm still calling shutdown_now() in that
        // case.
        //
        info!(self.log, "Stopping runtime");
        rt.shutdown_now().wait().unwrap();
        info!(self.log, "Runtime stopped successfully");
        self.is_running = false;
        debug!(self.log, "run() returned successfully");
    }
}

//
// Returns a mock event that can be used to bootstrap the watch loop. We use
// a NodeDataChanged event because the watch loop's response to this type of
// event is to get the data from the node being watched, which is also what
// we want to do when we start the watch loop anew.
//
fn mock_event() -> WatchedEvent {
    WatchedEvent {
        event_type: WatchedEventType::NodeDataChanged,
        //
        // This artificial keeper_state doesn't necessarily reflect reality, but
        // that's ok because it's paired with an artificial NodeDataChanged
        // event, and our handling for this type of event doesn't involve the
        // keeper_state field.
        //
        keeper_state: KeeperState::SyncConnected,
        //
        // We never use `path`, so we might as well set it to a clarifying
        // string in our artificially constructed WatchedEvent object.
        //
        path: "MOCK_EVENT_PATH".to_string(),
    }
}

//
// Parses the given zookeeper node data into a Backend object, compares it to
// the last Backend sent to the cueball connection pool, and sends it to the
// connection pool if the values differ.
//
// We need to extract two pieces of data from the "primary" json object in the
// json returned by zookeeper:
// * The backend's IP address
// * The backend's port
// The json object has an "ip" field, but not a port field. However, the port
// is contained in the "pgUrl" field, so this function extracts it from that.
// The port could also be extracted from the "id" field, but the pgUrl field is
// a convenient choice as it can be parsed structurally as a url and the port
// extracted from there.
//
// What this all means is: the resolver relies on the "primary.ip" and
// "primary.pgUrl" fields as an _interface_ to the zookeeper data. This feels a
// little ad-hoc and should be formalized and documented.
//
// # Arguments
//
// * `pool_tx` - The Sender upon which to send the update message
// * `new_value` - The raw zookeeper data we've newly retrieved
// * `last_backend` - The last Backend we sent to the connection pool
// * `log` - The Logger to be used for logging
//
fn process_value(
    pool_tx: &Sender<BackendMsg>,
    new_value: &[u8],
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    log: Logger,
) -> Result<(), ResolverError> {
    debug!(log, "process_value() entered");

    // Parse the bytes into a json object
    let v: SerdeJsonValue = match serde_json::from_slice(&new_value) {
        Ok(v) => v,
        Err(_) => {
            return Err(ResolverError::InvalidZkJson);
        }
    };

    //
    // Parse out the ip. We expect the json fields to exist, and return an error
    // if they don't, or if they are of the wrong type.
    //
    let ip = match &v["primary"]["ip"] {
        SerdeJsonValue::String(s) => match BackendAddress::from_str(s) {
            Ok(s) => s,
            Err(_) => {
                return Err(ResolverError::InvalidZkData(ZkDataField::Ip));
            }
        },
        SerdeJsonValue::Null => {
            return Err(ResolverError::MissingZkData(ZkDataField::Ip));
        }
        _ => {
            return Err(ResolverError::InvalidZkData(ZkDataField::Ip));
        }
    };

    //
    // Parse out the port. We expect the json fields to exist, and return an
    // error if they don't, or if they are of the wrong type.
    //
    let port = match &v["primary"]["pgUrl"] {
        SerdeJsonValue::String(s) => match Url::parse(s) {
            Ok(url) => match url.port() {
                Some(port) => port,
                None => {
                    return Err(ResolverError::MissingZkData(
                        ZkDataField::Port,
                    ));
                }
            },
            Err(_) => {
                return Err(ResolverError::InvalidZkData(
                    ZkDataField::PostgresUrl,
                ));
            }
        },
        SerdeJsonValue::Null => {
            return Err(ResolverError::MissingZkData(ZkDataField::PostgresUrl));
        }
        _ => {
            return Err(ResolverError::InvalidZkData(ZkDataField::PostgresUrl));
        }
    };

    // Construct a backend and key
    let backend = Backend::new(&ip, port);
    let backend_key = backend::srv_key(&backend);

    // Determine whether we need to send the new backend over
    let mut last_backend = last_backend.lock().unwrap();
    let should_send = match (*last_backend).clone() {
        Some(lb) => lb != backend_key,
        None => true,
    };

    // Send the new backend if necessary
    if should_send {
        info!(log, "New backend found; sending to connection pool";
            "backend" => LogItem(backend.clone()));
        if pool_tx
            .send(BackendMsg::AddedMsg(BackendAddedMsg {
                key: backend_key.clone(),
                backend,
            }))
            .is_err()
        {
            return Err(ResolverError::ConnectionPoolShutdown);
        }

        let lb_clone = (*last_backend).clone();
        *last_backend = Some(backend_key);

        //
        // Notify the connection pool that the old backend should be
        // removed, if the old backend is not None
        //
        if let Some(lbc) = lb_clone {
            info!(log, "Notifying connection pool of removal of old backend");
            if pool_tx
                .send(BackendMsg::RemovedMsg(BackendRemovedMsg(lbc)))
                .is_err()
            {
                return Err(ResolverError::ConnectionPoolShutdown);
            }
        }
    } else {
        info!(log, "New backend value does not differ; not sending");
    }
    debug!(log, "process_value() returned successfully");
    Ok(())
}

//
// Encapsulates all of the resolver state used in the futures context.
//
// Note that this struct's methods consume the instance of the struct they are
// called upon. The struct should be freely cloned to accommodate this.
//
#[derive(Clone)]
struct ResolverCore {
    // The Sender that this function should use to communicate with the cueball
    // connection pool
    pool_tx: Sender<BackendMsg>,
    // The key representation of the last backend sent to the cueball connection
    // pool. It will be updated by process_value() if we send a new backend over
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    // The addresses of the Zookeeper cluster the Resolver is connecting to,
    // along with associated state
    conn_str_state: Arc<Mutex<ZkConnectStringState>>,
    // The path to the cluster state node in zookeeper for the shard we're
    // watching
    cluster_state_path: String,
    // The exponential backoff state for the ZooKeeper connection
    conn_backoff: Arc<Mutex<ResolverBackoff>>,
    // The exponential backoff state for watching the ZooKeeper node
    watch_backoff: Arc<Mutex<ResolverBackoff>>,
    log: Logger,
}

impl ResolverCore {
    //
    // This function represents the body of the connect loop. It handles the
    // ZooKeeper connection, and calls watch_loop() to watch the ZooKeeper node.
    //
    // # Arguments
    //
    // * `delay` - The length of time to wait before attempting to connect
    //
    fn connect_loop(
        self,
        delay: Duration,
    ) -> impl Future<Item = Loop<(), Duration>, Error = ()> + Send {
        let log = self.log.clone();
        let oe_log = log.clone();
        let oe_conn_backoff = Arc::clone(&self.conn_backoff);
        let oe_conn_str_state = Arc::clone(&self.conn_str_state);

        Delay::new(Instant::now() + delay)
            .and_then(move |_| {
                let mut builder = ZooKeeperBuilder::default();
                builder.set_timeout(SESSION_TIMEOUT);
                builder.set_logger(log.new(o!(
                    "component" => "zookeeper"
                )));

                // Get the next address to connect to
                let mut state = self.conn_str_state.lock().unwrap();
                let addr = state.next_addr();
                drop(state);

                info!(log, "Connecting to ZooKeeper"; "addr" => addr);

                //
                // We expect() the result of get_addr_at() because we anticipate
                // the connect string having at least one element, and we can't
                // do anything useful if it doesn't.
                //
                builder
                    .connect(&addr)
                    .timeout(TCP_CONNECT_TIMEOUT)
                    .and_then(move |(zk, default_watcher)| {
                        info!(log, "Connected to ZooKeeper";
                            "addr" => addr);

                        //
                        // We've connected successfully, so reset the
                        // connection attempts
                        //
                        let mut state = self.conn_str_state.lock().unwrap();
                        state.reset_attempts();
                        drop(state);

                        //
                        // Main change-watching loop. A new loop iteration means
                        // we're setting a new watch (if necessary) and waiting
                        // for a result. Breaking from the loop means that we've
                        // hit some error and are returning control to the outer
                        // loop.
                        //
                        // Arg: WatchLoopState -- we set curr_event to an
                        //     artificially constructed WatchedEvent for the
                        //     first loop iteration, so the connection pool will
                        //     be initialized with the initial primary as its
                        //     backend.
                        // Loop::Break type: NextAction -- this value is used to
                        //     instruct the outer loop (this function) whether
                        //     to try to reconnect or terminate.
                        //
                        loop_fn(
                            WatchLoopState {
                                watcher: Box::new(default_watcher),
                                curr_event: mock_event(),
                                delay: NO_DELAY,
                            },
                            move |loop_state| {
                                //
                                // These fields require a new clone for every
                                // loop iteration, but they don't actually
                                // change from iteration to iteration, so
                                // they're not included as part of loop_state.
                                //
                                let loop_core = self.clone();
                                let zk = zk.clone();
                                loop_core
                                    .watch_loop(zk, loop_state)
                                    .map_err(TimeoutError::inner)
                            },
                        )
                        .and_then(move |next_action| {
                            ok(match next_action {
                                NextAction::Stop => Loop::Break(()),
                                //
                                // We reconnect immediately here instead of
                                // waiting, because if we're here it means that
                                // we came from the inner loop and thus we just
                                // had a valid connection terminate (as opposed
                                // to the `or_else` block below, were we've just
                                // tried to connect and failed), and thus
                                // there's no reason for us to delay trying to
                                // connect again.
                                //
                                NextAction::Reconnect(delay) => {
                                    Loop::Continue(delay)
                                }
                            })
                        })
                    })
                    .or_else(move |error| {
                        error!(oe_log, "Error connecting to ZooKeeper cluster";
                    "error" => LogItem(error));
                        ok(Loop::Continue(next_delay(
                            &oe_conn_backoff,
                            &DelayBehavior::CheckConnState(oe_conn_str_state),
                        )))
                    })
            })
            .map_err(|e| panic!("delay errored; err: {:?}", e))
    }

    //
    // This function represents the body of the watch loop. It both sets and
    // handles the watch, and calls process_value() to send new data to the
    // connection pool as the data arrives.
    //
    // This function can return from two states: before we've waited for the
    //  watch to fire (if we hit an error before waiting), or after we've waited
    // for the watch to fire (this could be a success or an error). These two
    // states require returning different Future types, so we wrap the returned
    // values in a future::Either to satisfy the type checker.
    //
    // # Arguments
    //
    // * `zk` - The ZooKeeper client object
    // * `loop_state`: The state to be passed to the next iteration of loop_fn()
    //
    fn watch_loop(
        self,
        zk: ZooKeeper,
        loop_state: WatchLoopState,
    ) -> impl Future<Item = Loop<NextAction, WatchLoopState>, Error = FailureError>
           + Send {
        let watcher = loop_state.watcher;
        let curr_event = loop_state.curr_event;
        let delay = loop_state.delay;
        let check_behavior =
            DelayBehavior::CheckConnState(self.conn_str_state.clone());
        //
        // TODO avoid mutex boilerplate from showing up in the log
        //
        let log = self.log.new(o!(
            "curr_event" => LogItem(curr_event.clone()),
            "delay" => LogItem(delay),
            "last_backend" => LogItem(Arc::clone(&self.last_backend))
        ));

        let oe_log = log.clone();
        let oe_conn_backoff = Arc::clone(&self.conn_backoff);
        let oe_check_behavior = check_behavior.clone();

        //
        // Helper function to handle the arcmut and the loop boilerplate
        //
        fn next_delay_action(
            backoff: &Arc<Mutex<ResolverBackoff>>,
            behavior: &DelayBehavior,
        ) -> Loop<NextAction, WatchLoopState> {
            Loop::Break(NextAction::Reconnect(next_delay(backoff, behavior)))
        }

        //
        // We set the watch here. If the previous iteration of the loop ended
        // because the keeper state changed rather than because the watch fired,
        // the watch will already have been set, so we don't _need_ to set it
        // here. With that said, it does no harm (zookeeper deduplicates watches
        // on the server side), and it may not be worth the effort to optimize
        // for this case, since keeper state changes (and, indeed, changes of
        // any sort) should happen infrequently.
        //
        info!(log, "Getting data");
        Delay::new(Instant::now() + delay)
            .and_then(move |_| {
                zk
            .watch()
            .get_data(&self.cluster_state_path)
            .and_then(move |(_, data)| {
                match curr_event.event_type {
                    // Keeper state has changed
                    WatchedEventType::None => {
                        match curr_event.keeper_state {
                            //
                            // TODO will these cases ever happen? Because if the
                            // keeper state is "bad", then get_data() will have
                            // failed and we won't be here.
                            //
                            KeeperState::Disconnected |
                            KeeperState::AuthFailed |
                            KeeperState::Expired => {
                                error!(log, "Keeper state changed; reconnecting";
                                    "keeper_state" =>
                                    LogItem(curr_event.keeper_state));
                                return Either::A(ok(next_delay_action(
                                    &self.conn_backoff,
                                    &check_behavior
                                )));
                            },
                            KeeperState::SyncConnected |
                            KeeperState::ConnectedReadOnly |
                            KeeperState::SaslAuthenticated => {
                                info!(log, "Keeper state changed";
                                    "keeper_state" =>
                                    LogItem(curr_event.keeper_state));
                            }
                        }
                    },
                    // The data watch fired
                    WatchedEventType::NodeDataChanged => {
                        //
                        // We didn't get the data, which means the node doesn't
                        // exist yet. We should wait a bit and try again. We'll
                        // just use the same event as before.
                        //
                        if data.is_none() {
                            info!(log, "ZK data does not exist yet");
                            let delay = next_delay(
                                &self.watch_backoff,
                                &DelayBehavior::AlwaysWait
                            );
                            return Either::A(ok(Loop::Continue(WatchLoopState {
                                watcher,
                                curr_event,
                                delay,
                            })));
                        }
                        //
                        // Discard the Stat from the data, as we don't use it.
                        //
                        let data = data.unwrap().0;
                        info!(log, "got data"; "data" => LogItem(data.clone()));
                        match process_value(
                            &self.pool_tx.clone(),
                            &data,
                            Arc::clone(&self.last_backend),
                            log.clone()
                        ) {
                            Ok(_) => {},
                            Err(e) => {
                                error!(log, ""; "error" => LogItem(e.clone()));
                                //
                                // The error is between the client and the
                                // outward-facing channel, not between the
                                // client and the zookeeper connection, so we
                                // don't have to attempt to reconnect here and
                                // can continue, unless the error tells us to
                                // stop.
                                //
                                if e.should_stop() {
                                    return Either::A(ok(Loop::Break(
                                        NextAction::Stop)));
                                }
                            }
                        }
                    },
                    WatchedEventType::NodeDeleted => {
                        //
                        // The node doesn't exist, but we can't use the existing
                        // event, or we'll just loop on this case forever. We
                        // use the mock event instead.
                        //
                        info!(log, "ZK node deleted");
                        let delay = next_delay(
                            &self.watch_backoff,
                            &DelayBehavior::AlwaysWait
                        );
                        return Either::A(ok(Loop::Continue(WatchLoopState {
                            watcher,
                            curr_event: mock_event(),
                            delay,
                        })));
                    },
                    e => panic!("Unexpected event received: {:?}", e)
                };

                //
                // If we got here, we're waiting for the watch to fire. Before
                // this point, we wrap the return value in Either::A. After this
                // point, we wrap the return value in Either::B. See the comment
                // about "Either" some lines above.
                //
                let oe_log = log.clone();
                let oe_conn_backoff = Arc::clone(&self.conn_backoff);
                let oe_check_behavior = check_behavior.clone();

                info!(log, "Watching for change");
                Either::B(watcher.into_future()
                    .and_then(move |(event, watcher)| {
                        let loop_next = match event {
                            Some(event) => {
                                info!(log, "change event received; looping to \
                                    process event"; "event" =>
                                    LogItem(event.clone()));
                                Loop::Continue(WatchLoopState {
                                    watcher,
                                    curr_event: event,
                                    delay: NO_DELAY
                                })
                            },
                            //
                            // If we didn't get a valid event, this means the
                            // Stream got closed, which indicates a connection
                            // issue, so we reconnect.
                            //
                            None => {
                                error!(log, "Event stream closed; reconnecting");
                                next_delay_action(
                                    &self.conn_backoff,
                                    &check_behavior
                                )
                            }
                        };
                        ok(loop_next)
                    })
                    .or_else(move |_| {
                        //
                        // If we get an error from the event Stream, we assume
                        // that something went wrong with the zookeeper
                        // connection and attempt to reconnect.
                        //
                        // The stream's error type is (), so there's no
                        // information to extract from it.
                        //
                        error!(oe_log, "Error received from event stream");
                        ok(next_delay_action(
                            &oe_conn_backoff,
                            &oe_check_behavior
                        ))
                    }))
            })
            //
            // If some error occurred getting the data, we assume we should
            // reconnect to the zookeeper server.
            //
            .or_else(move |error| {
                error!(oe_log, "Error getting data"; "error" => LogItem(error));
                ok(next_delay_action(
                    &oe_conn_backoff,
                    &oe_check_behavior
                ))
            })
            })
            .map_err(|e| panic!("delay errored; err: {:?}", e))
    }
}

//
// Unit tests
//
// The "." path attribute below is so paths within the `test` submodule will be
// relative to "src" rather than "src/test", which does not exist. This allows
// us to import the "../tests/test_data.rs" module here.
//
#[path = "."]
#[cfg(test)]
mod test {
    use super::*;

    use std::iter;
    use std::sync::mpsc::TryRecvError;
    use std::sync::{Arc, Mutex};
    use std::vec::Vec;

    use quickcheck::{quickcheck, Arbitrary, Gen};

    use common::{test_data, util};

    impl Arbitrary for ZkConnectString {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let size = usize::arbitrary(g);
            ZkConnectString(
                iter::repeat(())
                    .map(|()| SocketAddr::arbitrary(g))
                    .take(size)
                    .collect(),
            )
        }
    }

    //
    // Test parsing ZkConnectString from string
    //
    quickcheck! {
        fn prop_zk_connect_string_parse(
            conn_str: ZkConnectString
        ) -> bool
        {
            //
            // We expect an error only if the input string was zero-length
            //
            match ZkConnectString::from_str(&conn_str.to_string()) {
                Ok(cs) => cs == conn_str,
                _ => conn_str.to_string() == ""
            }
        }
    }

    // Below: test process_value()

    //
    // Represents a process_value test case, including inputs and expected
    // outputs.
    //
    struct ProcessValueFields {
        value: Vec<u8>,
        last_backend: BackendKey,
        expected_error: Option<ResolverError>,
        added_backend: Option<BackendAddedMsg>,
        removed_backend: Option<BackendRemovedMsg>,
    }

    //
    // Run a process_value test case
    //
    fn run_process_value_fields(input: ProcessValueFields) {
        let (tx, rx) = channel();
        let last_backend = Arc::new(Mutex::new(Some(input.last_backend)));

        let result = process_value(
            &tx.clone(),
            &input.value,
            last_backend,
            util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap(),
        );
        match input.expected_error {
            None => assert_eq!(result, Ok(())),
            Some(expected_error) => assert_eq!(result, Err(expected_error)),
        }

        let mut received_messages = Vec::new();

        let expected_message_count = {
            let mut acc = 0;
            if input.added_backend.is_some() {
                acc += 1;
            }
            if input.removed_backend.is_some() {
                acc += 1;
            }
            acc
        };

        // Receive as many messages as we expect
        for i in 0..expected_message_count {
            let channel_result = rx.try_recv();
            match channel_result {
                Err(e) => panic!(
                    "Unexpected error receiving on channel: {:?} \
                     -- Loop iteration: {:?}",
                    e, i
                ),
                Ok(result) => {
                    received_messages.push(result);
                }
            }
        }

        //
        // Make sure there are not more messages than we expect on the channel.
        // Can't use assert_eq! here because BackendMsg doesn't implement Debug.
        //
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            _ => panic!("Unexpected message on resolver channel"),
        }

        // Check that the "added" message was received if applicable
        if let Some(msg) = input.added_backend {
            let msg = BackendMsg::AddedMsg(msg);
            match util::find_msg_match(&received_messages, &msg) {
                None => panic!("added_backend not found in received messages"),
                Some(index) => {
                    received_messages.remove(index);
                    ()
                }
            }
        }

        // Check that the "removed" message was received if applicable
        if let Some(msg) = input.removed_backend {
            let msg = BackendMsg::RemovedMsg(msg);
            match util::find_msg_match(&received_messages, &msg) {
                None => {
                    panic!("removed_backend not found in received messages")
                }
                Some(index) => {
                    received_messages.remove(index);
                    ()
                }
            }
        }
    }

    #[test]
    fn process_value_test_port_ip_change() {
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip2_port2();

        run_process_value_fields(ProcessValueFields {
            value: data_2.raw_vec(),
            last_backend: data_1.key(),
            expected_error: None,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg()),
        });
    }

    #[test]
    fn process_value_test_port_change() {
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip2_port1();

        run_process_value_fields(ProcessValueFields {
            value: data_2.raw_vec(),
            last_backend: data_1.key(),
            expected_error: None,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg()),
        });
    }

    #[test]
    fn process_value_test_ip_change() {
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip1_port2();

        run_process_value_fields(ProcessValueFields {
            value: data_2.raw_vec(),
            last_backend: data_1.key(),
            expected_error: None,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg()),
        });
    }

    #[test]
    fn process_value_test_no_change() {
        let data = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: data.raw_vec(),
            last_backend: data.key(),
            expected_error: None,
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_no_ip() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::no_ip_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(ZkDataField::Ip)),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_wrong_type_ip() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::wrong_type_ip_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::Ip)),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_invalid_ip() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::invalid_ip_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::Ip)),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_no_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::no_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(
                ZkDataField::PostgresUrl,
            )),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_wrong_type_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::wrong_type_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(
                ZkDataField::PostgresUrl,
            )),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_invalid_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::invalid_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(
                ZkDataField::PostgresUrl,
            )),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_no_port_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::no_port_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(
                ZkDataField::Port,
            )),
            added_backend: None,
            removed_backend: None,
        });
    }

    #[test]
    fn process_value_test_invalid_json() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields {
            value: test_data::invalid_json_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkJson),
            added_backend: None,
            removed_backend: None,
        });
    }
}
