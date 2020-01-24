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
use std::fmt::Debug;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version};
use failure::Error as FailureError;
use futures::future::{ok, loop_fn, Either, Future, Loop};
use futures::stream::Stream;
use itertools::Itertools;
use serde::Deserialize;
use serde_json;
use serde_json::Value as SerdeJsonValue;
use slog::{
    error,
    info,
    debug,
    o,
    Drain,
    Key,
    LevelFilter,
    Logger,
    Record,
    Serializer
};
use slog::Result as SlogResult;
use slog::Value as SlogValue;
use tokio_zookeeper::{
    KeeperState,
    WatchedEvent,
    WatchedEventType,
    ZooKeeper,
    ZooKeeperBuilder
};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::timer::Delay;
use tokio::timer::timeout::Error as TimeoutError;
use url::Url;

use cueball::backend::{
    self,
    BackendAddress,
    BackendKey,
    Backend,
};
use cueball::resolver::{
    BackendAddedMsg,
    BackendRemovedMsg,
    BackendMsg,
    Resolver
};

pub mod common;

//
// The interval at which the resolver should send heartbeats via the
// connection pool channel. Public for use in tests.
//
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

//
// Timeout for zookeeper sessions. Public for use in tests.
//
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(5);

//
// Timeout for the initial tcp connect operation to zookeeper. Public for use in
// tests.
//
pub const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

//
// Delays to be used when reconnecting to the zookeeper client. Public for use
// in tests.
//
pub const RECONNECT_DELAY: Duration = Duration::from_secs(10);
pub const RECONNECT_NODELAY: Duration = Duration::from_secs(0);

//
// Delays to be used when re-setting the watch on the zookeeper node. Public for
// use in tests.
//
pub const WATCH_LOOP_DELAY: Duration = Duration::from_secs(10);
pub const WATCH_LOOP_NODELAY: Duration = Duration::from_secs(0);

//
// An error type to be used internally.
//
#[derive(Clone, Debug, PartialEq)]
enum ResolverError {
    InvalidZkJson,
    InvalidZkData(ZkDataField),
    MissingZkData(ZkDataField),
    ConnectionPoolShutdown
}

impl ResolverError {
    ///
    /// This function provides a means of determining whether or not a given
    /// error should cause the resolver to stop.
    ///
    fn should_stop(&self) -> bool {
        match self {
            ResolverError::ConnectionPoolShutdown => true,
            _ => false
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ZkDataField {
    Ip,
    Port,
    PostgresUrl
}

#[derive(Debug)]
pub enum ZkConnectStringError {
    EmptyString,
    MalformedAddr
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
    fn get_addr_at(&self, index: usize) -> Option<&SocketAddr> {
        self.0.get(index)
    }
}

impl ToString for ZkConnectString {
    fn to_string(&self) -> String {
        self
            .0
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
            .fold(acc, |acc, x| {
                match (acc, x) {
                    (Ok(mut addrs), Ok(addr)) => {
                        addrs.push(addr);
                        Ok(addrs)
                    },
                    (Err(e), _) => Err(e),
                    (_, Err(e)) => Err(ZkConnectStringError::from(e))
                }
            })
            .and_then(|x| Ok(ZkConnectString(x)))
    }
}

///
/// A serializable type to be used in log entries. Wraps around any type that
/// implements Debug and uses the Debug representation of the type as the
/// serialized output.
///
struct LogItem<T>(T) where T: Debug;

impl<T: Debug> SlogValue for LogItem<T> {
    fn serialize(&self, _rec: &Record, key: Key,
        serializer: &mut dyn Serializer) -> SlogResult {
            serializer.emit_str(key, &format!("{:?}", self.0))
    }
}

// Represents an action to be taken in the event of a connection error.
enum NextAction {
    //
    // The Duration field is the amount of time to wait before
    // reconnecting.
    //
    Reconnect(Duration),
    Stop,
}

//
// Encapsulates the state that one iteration of the watch loop passes
// to the next iteration.
//
struct WatchLoopState {
    watcher: Box<dyn Stream
        <Item = WatchedEvent, Error = ()> + Send>,
    curr_event: WatchedEvent,
    delay: Duration
}

#[derive(Debug)]
pub struct ManateePrimaryResolver {
    ///
    /// The addresses of the Zookeeper cluster the Resolver is connecting to
    ///
    connect_string: ZkConnectString,
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
    log: Logger
}

impl ManateePrimaryResolver {
    ///
    /// Creates a new ManateePrimaryResolver instance.
    ///
    /// # Arguments
    ///
    /// * `connect_string` - a comma-separated list of the zookeeper instances
    ///   in the cluster
    /// * `path` - The path to the root node in zookeeper for the shard we're
    ///    watching
    ///
    pub fn new(
        connect_string: ZkConnectString,
        path: String,
        log: Option<Logger>
    ) -> Self
    {
        let cluster_state_path = [&path, "/state"].concat();

        //
        // Add the log_values to the passed-in logger, or create a new logger if
        // the caller did not pass one in
        //
        let log = log.unwrap_or_else(|| {
            Logger::root(Mutex::new(LevelFilter::new(
                slog_bunyan::with_name(crate_name!(),
                    std::io::stdout()).build(),
                slog::Level::Info)).fuse(),
                o!("build-id" => crate_version!()))
        });

        ManateePrimaryResolver {
            connect_string: connect_string.clone(),
            cluster_state_path,
            last_backend: Arc::new(Mutex::new(None)),
            is_running: false,
            log
        }
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
        // We never use `path`, so we might as well set it to an
        // empty string in our artificially constructed WatchedEvent
        // object.
        //
        path: "".to_string(),
    }
}
///
/// Parses the given zookeeper node data into a Backend object, compares it to
/// the last Backend sent to the cueball connection pool, and sends it to the
/// connection pool if the values differ.
///
/// We need to extract two pieces of data from the "primary" json object in the
/// json returned by zookeeper:
/// * The backend's IP address
/// * The backend's port
/// The json object has an "ip" field, but not a port field. However, the port
/// is contained in the "pgUrl" field, so this function extracts it from that.
/// The port could also be extracted from the "id" field, but the pgUrl field is
/// a convenient choice as it can be parsed structurally as a url and the port
/// extracted from there.
///
/// What this all means is: the resolver relies on the "primary.ip" and
/// "primary.pgUrl" fields as an _interface_ to the zookeeper data. This feels a
/// little ad-hoc and should be formalized and documented.
///
/// # Arguments
///
/// * `pool_tx` - The Sender upon which to send the update message
/// * `new_value` - The raw zookeeper data we've newly retrieved
/// * `last_backend` - The last Backend we sent to the connection pool
/// * `log` - The Logger to be used for logging
///
fn process_value(
    pool_tx: &Sender<BackendMsg>,
    new_value: &[u8],
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    log: Logger
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
        SerdeJsonValue::String(s) => {
            match BackendAddress::from_str(s) {
                Ok(s) => s,
                Err(_) => {
                    return Err(ResolverError::InvalidZkData(ZkDataField::Ip));
                }
            }
        },
        SerdeJsonValue::Null => {
            return Err(ResolverError::MissingZkData(ZkDataField::Ip));
        },
        _ => {
            return Err(ResolverError::InvalidZkData(ZkDataField::Ip));
        }
    };

    //
    // Parse out the port. We expect the json fields to exist, and return an
    // error if they don't, or if they are of the wrong type.
    //
    let port = match &v["primary"]["pgUrl"] {
        SerdeJsonValue::String(s) => {
            match Url::parse(s) {
                Ok(url) => {
                    match url.port() {
                        Some(port) => port,
                        None => {
                            return Err(ResolverError::MissingZkData(
                                ZkDataField::Port));
                        }
                    }
                },
                Err(_) => {
                    return Err(ResolverError::InvalidZkData(
                        ZkDataField::PostgresUrl))
                }
            }
        },
        SerdeJsonValue::Null => {
            return Err(ResolverError::MissingZkData(ZkDataField::PostgresUrl));
        },
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
        if pool_tx.send(BackendMsg::AddedMsg(BackendAddedMsg {
            key: backend_key.clone(),
            backend
        })).is_err() {
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
            if pool_tx.send(BackendMsg::RemovedMsg(
                BackendRemovedMsg(lbc))).is_err() {
                return Err(ResolverError::ConnectionPoolShutdown);
            }
        }
    } else {
        info!(log, "New backend value does not differ; not sending");
    }
    debug!(log, "process_value() returned successfully");
    Ok(())
}

///
/// This function represents the body of the watch loop. It both sets and
/// handles the watch, and calls process_value() to send new data to the
/// connection pool as the data arrives.
///
/// This function can return from two states: before we've waited for the
///  watch to fire (if we hit an error before waiting), or after we've waited
/// for the watch to fire (this could be a success or an error). These two
/// states require returning different Future types, so we wrap the returned
/// values in a future::Either to satisfy the type checker.
///
/// # Arguments
///
/// * `pool_tx` - The Sender upon which to send the update message
/// * `last_backend` - The last Backend we sent to the connection pool
/// * `cluster_state_path` - The path to the cluster state node in zookeeper for
///   the shard we're watching
/// * `zk` - The ZooKeeper client object
/// * `loop_state`: The state to be passed to the next iteration of loop_fn()
/// * `log` - The Logger to be used for logging
///
fn watch_loop(
    pool_tx: Sender<BackendMsg>,
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    cluster_state_path: String,
    zk: ZooKeeper,
    loop_state: WatchLoopState,
    log: Logger
) -> impl Future<Item = Loop<NextAction, WatchLoopState>,
    Error = FailureError> + Send {

    let watcher = loop_state.watcher;
    let curr_event = loop_state.curr_event;
    let delay = loop_state.delay;
    //
    // TODO avoid mutex boilerplate from showing up in the log
    //
    let log = log.new(o!(
        "curr_event" => LogItem(curr_event.clone()),
        "delay" => LogItem(delay),
        "last_backend" => LogItem(Arc::clone(&last_backend))
    ));

    info!(log, "Getting data");
    let oe_log = log.clone();

    //
    // We set the watch here. If the previous iteration of the loop ended
    // because the keeper state changed rather than because the watch fired, the
    // watch will already have been set, so we don't _need_ to set it here. With
    // that said, it does no harm (zookeeper deduplicates watches on the server
    // side), and it may not be worth the effort to optimize for this case,
    // since keeper state changes (and, indeed, changes of any sort) should
    // happen infrequently.
    //
    Delay::new(Instant::now() + delay)
    .and_then(move |_| {
        zk
        .watch()
        .get_data(&cluster_state_path)
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
                            return Either::A(ok(Loop::Break(
                                NextAction::Reconnect(RECONNECT_NODELAY))));
                        },
                        KeeperState::SyncConnected |
                        KeeperState::ConnectedReadOnly |
                        KeeperState::SaslAuthenticated => {
                            info!(log, "Keeper state changed"; "keeper_state" =>
                                LogItem(curr_event.keeper_state));
                        }
                    }
                },
                // The data watch fired
                WatchedEventType::NodeDataChanged => {
                    //
                    // We didn't get the data, which means the node doesn't
                    // exist yet. We should wait a bit and try again. We'll just
                    // use the same event as before.
                    //
                    if data.is_none() {
                        info!(log, "ZK data does not exist yet");
                        return Either::A(ok(Loop::Continue(WatchLoopState {
                            watcher,
                            curr_event,
                            delay: WATCH_LOOP_DELAY
                        })));
                    }
                    //
                    // Discard the Stat from the data, as we don't use it.
                    //
                    let data = data.unwrap().0;
                    info!(log, "got data"; "data" => LogItem(data.clone()));
                    match process_value(
                        &pool_tx.clone(),
                        &data,
                        Arc::clone(&last_backend),
                        log.clone()
                    ) {
                        Ok(_) => {},
                        Err(e) => {
                            error!(log, ""; "error" => LogItem(e.clone()));
                            //
                            // The error is between the client and the
                            // outward-facing channel, not between the client
                            // and the zookeeper connection, so we don't have to
                            // attempt to reconnect here and can continue,
                            // unless the error tells us to stop.
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
                    // event, or we'll just loop on this case forever. We use
                    // the mock event instead.
                    //
                    info!(log, "ZK node deleted");
                    return Either::A(ok(Loop::Continue(WatchLoopState {
                        watcher,
                        curr_event: mock_event(),
                        delay: WATCH_LOOP_DELAY
                    })));
                },
                e => panic!("Unexpected event received: {:?}", e)
            };

            //
            // If we got here, we're waiting for the watch to fire. Before this
            // point, we wrap the return value in Either::A. After this point,
            // we wrap the return value in Either::B. See the comment about
            // "Either" some lines above.
            //
            info!(log, "Watching for change");
            let oe_log = log.clone();
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
                                delay: WATCH_LOOP_NODELAY
                            })
                        },
                        //
                        // If we didn't get a valid event, this means the Stream
                        // got closed, which indicates a connection issue, so we
                        // reconnect.
                        //
                        None => {
                            error!(log, "Event stream closed; reconnecting");
                            Loop::Break(NextAction::Reconnect(
                                RECONNECT_NODELAY))
                        }
                    };
                    ok(loop_next)
                })
                .or_else(move |_| {
                    //
                    // If we get an error from the event Stream, we assume that
                    // something went wrong with the zookeeper connection and
                    // attempt to reconnect.
                    //
                    // The stream's error type is (), so there's no information
                    // to extract from it.
                    //
                    error!(oe_log, "Error received from event stream");
                    ok(Loop::Break(NextAction::Reconnect(RECONNECT_NODELAY)))
                }))
        })
        //
        // If some error occurred getting the data, we assume we should
        // reconnect to the zookeeper server.
        //
        .or_else(move |error| {
            error!(oe_log, "Error getting data"; "error" => LogItem(error));
            ok(Loop::Break(NextAction::Reconnect(RECONNECT_NODELAY)))
        })
    })
    .map_err(|e| panic!("delay errored; err: {:?}", e))
}

fn connect_loop(
    pool_tx: Sender<BackendMsg>,
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    connect_string: ZkConnectString,
    cluster_state_path: String,
    delay: Duration,
    log: Logger
) -> impl Future<Item = Loop<(), Duration>,
    Error = ()> + Send {

    let oe_log = log.clone();

    Delay::new(Instant::now() + delay)
    .and_then(move |_| {
        info!(log, "Connecting to ZooKeeper cluster");

        let mut builder = ZooKeeperBuilder::default();
        builder.set_timeout(SESSION_TIMEOUT);
        builder.set_logger(log.new(o!(
            "component" => "zookeeper"
        )));

        //
        // We expect() the result of get_addr_at() because we anticipate the
        // connect string having at least one element, and we can't do anything
        // useful if it doesn't.
        //
        builder.connect(connect_string.get_addr_at(0)
            .expect("connect_string should have at least one IP address"))
        .timeout(TCP_CONNECT_TIMEOUT)
        .and_then(move |(zk, default_watcher)| {
            info!(log, "Connected to ZooKeeper cluster");

            //
            // Main change-watching loop. A new loop iteration means we're
            // setting a new watch (if necessary) and waiting for a result.
            // Breaking from the loop means that we've hit some error and are
            // returning control to the outer loop.
            //
            // Arg: WatchLoopState -- we set curr_event to an artificially
            //     constructed WatchedEvent for the first loop iteration, so the
            //     connection pool will be initialized with the initial primary
            //     as its backend.
            // Loop::Break type: NextAction -- this value is used to instruct
            //     the outer loop (this function) whether to try to reconnect or
            //     terminate.
            //
            loop_fn(WatchLoopState {
                watcher: Box::new(default_watcher),
                curr_event: mock_event(),
                delay: WATCH_LOOP_NODELAY
            } , move |loop_state| {
                //
                // These fields require a new clone for every loop iteration,
                // but they don't actually change from iteration to iteration,
                // so they're not included as part of loop_state.
                //
                let pool_tx = pool_tx.clone();
                let last_backend = Arc::clone(&last_backend);
                let cluster_state_path = cluster_state_path.clone();
                let zk = zk.clone();
                let log = log.clone();

                watch_loop(
                    pool_tx,
                    last_backend,
                    cluster_state_path,
                    zk,
                    loop_state,
                    log
                )
                .map_err(TimeoutError::inner)
            })
            .and_then(|next_action| {
                ok(match next_action {
                    NextAction::Stop => Loop::Break(()),
                    //
                    // We reconnect immediately here instead of waiting, because
                    // if we're here it means that we came from the inner loop
                    // and thus we just had a valid connection terminate (as
                    // opposed to the `or_else` block below, were we've just
                    // tried to connect and failed), and thus there's no reason
                    // for us to delay trying to connect again.
                    //
                    NextAction::Reconnect(delay) => Loop::Continue(delay)
                })
            })
        })
        .or_else(move |error| {
            error!(oe_log, "Error connecting to ZooKeeper cluster";
                "error" => LogItem(error));

            ok(Loop::Continue(RECONNECT_DELAY))
        })
    })
    .map_err(|e| panic!("delay errored; err: {:?}", e))
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

        //
        // Variables moved to tokio runtime thread:
        //
        // * `connect_string` - A comma-separated list of the zookeeper
        //   instances in the cluster
        // * `cluster_state_path` - The path to the cluster state node in
        //   zookeeper for the shard we're watching
        // * `pool_tx` - The Sender that this function should use to communicate
        //   with the cueball connection pool
        // * `last_backend` - The key representation of the last backend sent to
        //   the cueball connection pool. It will be updated by process_value()
        //   if we send a new backend over.
        // * log - A clone of the resolver's master log
        // * at_log - Another clone, used in the `and_then` portion of the loop
        //
        let connect_string = self.connect_string.clone();
        let cluster_state_path = self.cluster_state_path.clone();
        let pool_tx = s.clone();
        let last_backend = Arc::clone(&self.last_backend);
        let log = self.log.clone();
        let at_log = self.log.clone();

        let exited = Arc::new(AtomicBool::new(false));
        let exited_clone = Arc::clone(&exited);

        //
        // Start the event-processing task. This is structured as two nested
        // loops: one to handle the zookeeper connection and one to handle
        // setting the watch. These are handled by the connect_loop() and
        // watch_loop() functions, respectively
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
            loop_fn(RECONNECT_NODELAY, move |delay| {
                let pool_tx = pool_tx.clone();
                let last_backend = Arc::clone(&last_backend);
                let connect_string = connect_string.clone();
                let cluster_state_path = cluster_state_path.clone();
                let log = log.clone();

                connect_loop(
                    pool_tx,
                    last_backend,
                    connect_string,
                    cluster_state_path,
                    delay,
                    log
                )
            }).and_then(move |_| {
                info!(at_log, "Event-processing task stopping");
                exited_clone.store(true, Ordering::Relaxed);
                Ok(())
            })
            .map(|_| ())
            .map_err(|_| {
                unreachable!("connect_loop() should never return an error")
            })
        );

        //
        // Heartbeat-sending loop. If we break from this loop, the resolver
        // exits.
        //
        loop {
            if exited.load(Ordering::Relaxed) {
                info!(self.log,
                    "event-processing task exited; stopping heartbeats");
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
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc::{channel, TryRecvError};
    use std::vec::Vec;

    use quickcheck::{quickcheck, Arbitrary, Gen};

    use common::{util, test_data};

    impl Arbitrary for ZkConnectString {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let size = usize::arbitrary(g);
            ZkConnectString(
                iter::repeat(())
                    .map(|()| SocketAddr::arbitrary(g))
                    .take(size)
                    .collect()
            )
        }
    }

    //
    // Test parsing ZkConnectString from string
    //
    quickcheck! {
        fn prop_zk_connect_string_parse(
            connect_string: ZkConnectString
        ) -> bool
        {
            //
            // We expect an error only if the input string was zero-length
            //
            match ZkConnectString::from_str(&connect_string.to_string()) {
                Ok(cs) => cs == connect_string,
                _ => connect_string.to_string() == ""
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
        removed_backend: Option<BackendRemovedMsg>
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
            util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap());
        match input.expected_error {
            None => assert_eq!(result, Ok(())),
            Some(expected_error) => {
                assert_eq!(result, Err(expected_error))
            }
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
                Err(e) => panic!("Unexpected error receiving on channel: {:?} \
                    -- Loop iteration: {:?}", e, i),
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
            _ => panic!("Unexpected message on resolver channel")
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
                None =>
                    panic!("removed_backend not found in received messages"),
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

        run_process_value_fields(ProcessValueFields{
            value: data_2.raw_vec(),
            last_backend: data_1.key(),
            expected_error: None,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        });
    }

    #[test]
    fn process_value_test_port_change() {
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip2_port1();

        run_process_value_fields(ProcessValueFields{
            value: data_2.raw_vec(),
            last_backend: data_1.key(),
            expected_error: None,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        });
    }

    #[test]
    fn process_value_test_ip_change() {
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip1_port2();

        run_process_value_fields(ProcessValueFields{
            value: data_2.raw_vec(),
            last_backend: data_1.key(),
            expected_error: None,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        });
    }

    #[test]
    fn process_value_test_no_change() {
        let data = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: data.raw_vec(),
            last_backend: data.key(),
            expected_error: None,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_no_ip() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::no_ip_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(ZkDataField::Ip)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_wrong_type_ip() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::wrong_type_ip_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::Ip)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_invalid_ip() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::invalid_ip_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::Ip)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_no_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::no_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(
                ZkDataField::PostgresUrl)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_wrong_type_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::wrong_type_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(
                ZkDataField::PostgresUrl)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_invalid_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::invalid_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(
                ZkDataField::PostgresUrl)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_no_port_pg_url() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::no_port_pg_url_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(
                ZkDataField::Port)),
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn process_value_test_invalid_json() {
        let filler = test_data::backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: test_data::invalid_json_vec(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkJson),
            added_backend: None,
            removed_backend: None
        });
    }
 }
