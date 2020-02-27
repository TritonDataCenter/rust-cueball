//
// Copyright 2020 Joyent, Inc.
//

use std::convert::From;
use std::default::Default;
use std::env;
use std::io::Error as StdIoError;
use std::process::Command;
use std::str::FromStr;
use std::sync::mpsc::{
    channel, Receiver, RecvError, RecvTimeoutError, TryRecvError,
};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version};
use cueball::resolver::{
    BackendAddedMsg, BackendMsg, BackendRemovedMsg, Resolver,
};
use failure::Error as FailureError;
use slog::{o, Drain, Level, LevelFilter, Logger};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio_zookeeper::error as tokio_zk_error;
use tokio_zookeeper::{Acl, CreateMode, ZooKeeper};
use uuid::Uuid;

use super::test_data;
use crate::{ManateePrimaryResolver, ZkConnectString};

pub const DEFAULT_LOG_LEVEL: Level = Level::Info;
pub const LOG_LEVEL_ENV_VAR: &str = "RESOLVER_LOG_LEVEL";

pub const RESOLVER_STARTUP_DELAY: Duration = Duration::from_secs(1);
pub const STANDARD_RECV_TIMEOUT: Duration = Duration::from_secs(1);
pub const SLACK_DURATION: Duration = Duration::from_millis(10);

#[derive(Debug, PartialEq)]
pub enum ZkStatus {
    Enabled,
    Disabled,
}

#[derive(Debug)]
pub enum TestError {
    RecvTimeout,
    UnexpectedMessage,
    BackendMismatch(String),
    ZkError(String),
    InvalidTimeout,
    DisconnectError,
    SubprocessError(String),
    IncorrectBehavior(String),
    InvalidLogLevel,
    UnspecifiedError,
}

impl From<RecvTimeoutError> for TestError {
    fn from(_: RecvTimeoutError) -> Self {
        TestError::RecvTimeout
    }
}

impl From<TryRecvError> for TestError {
    fn from(_: TryRecvError) -> Self {
        TestError::DisconnectError
    }
}

impl From<RecvError> for TestError {
    fn from(_: RecvError) -> Self {
        TestError::DisconnectError
    }
}

impl From<tokio_zk_error::Delete> for TestError {
    fn from(e: tokio_zk_error::Delete) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<tokio_zk_error::Create> for TestError {
    fn from(e: tokio_zk_error::Create) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<tokio_zk_error::SetData> for TestError {
    fn from(e: tokio_zk_error::SetData) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<FailureError> for TestError {
    fn from(e: FailureError) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<StdIoError> for TestError {
    fn from(e: StdIoError) -> Self {
        TestError::SubprocessError(format!("{:?}", e))
    }
}

impl From<()> for TestError {
    fn from(_: ()) -> Self {
        TestError::UnspecifiedError
    }
}

//
// A struct that encapsulates the context for a given test. Provides many
// convenience methods.
//
pub struct TestContext {
    pub connect_string: ZkConnectString,
    pub root_path: String,
    rt: Runtime,
}

impl Default for TestContext {
    fn default() -> Self {
        let connect_string =
            ZkConnectString::from_str("127.0.0.1:2181").unwrap();
        let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
        TestContext::new(connect_string, root_path)
    }
}

impl TestContext {
    pub fn new(connect_string: ZkConnectString, root_path: String) -> Self {
        TestContext {
            connect_string,
            root_path,
            rt: Runtime::new().expect("Error creating tokio runtime"),
        }
    }

    //
    // This function (and the accompanying teardown function below) can safely
    // be called multiple times throughout the course of the test
    //
    pub fn setup_zk_nodes(&mut self) -> Result<(), TestError> {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";
        self.create_zk_node(root_path, Vec::new())?;
        self.create_zk_node(data_path, Vec::new())?;
        Ok(())
    }

    pub fn teardown_zk_nodes(&mut self) -> Result<(), TestError> {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";

        // The order here matters -- we must delete the child before the parent!
        self.delete_zk_node(data_path)?;
        self.delete_zk_node(root_path)?;
        Ok(())
    }

    //
    // This function should be called exactly once, at the end of the test. It
    // consumes the TestContext.
    //
    fn finalize(self) -> Result<(), TestError> {
        // Wait for the futures to resolve
        self.rt.shutdown_on_idle().wait()?;
        Ok(())
    }

    fn create_zk_node(
        &mut self,
        path: String,
        data: Vec<u8>,
    ) -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(
                &self
                    .connect_string
                    .get_addr_at(0)
                    .expect("Empty connect string"),
            )
            .and_then(move |(zk, _)| {
                zk.create(
                    &path,
                    data,
                    Acl::open_unsafe(),
                    CreateMode::Persistent,
                )
                .map(|(_, res)| res)
            }), //
                // A double question mark! The first one unwraps the error
                // returned by ZooKeeper::connect; the second one unwraps the error
                // returned by create().
                //
        )??;
        Ok(())
    }

    fn set_zk_data(
        &mut self,
        path: String,
        data: Vec<u8>,
    ) -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(
                &self
                    .connect_string
                    .get_addr_at(0)
                    .expect("Empty connect string"),
            )
            .and_then(move |(zk, _)| {
                zk.set_data(&path, None, data).map(|(_, res)| res)
            }), //
                // Another double question mark! The first one unwraps the error
                // returned by ZooKeeper::connect; the second one unwraps the error
                // returned by set_data().
                //
        )??;

        Ok(())
    }

    fn delete_zk_node(&mut self, path: String) -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(
                &self
                    .connect_string
                    .get_addr_at(0)
                    .expect("Empty connect string"),
            )
            .and_then(move |(zk, _)| {
                zk.delete(&path, None).map(|(_, res)| res)
            }),
        )??;
        //
        // Another double question mark! The first one unwraps the error
        // returned by ZooKeeper::connect; the second one unwraps the error
        // returned by delete().
        //
        Ok(())
    }

    //
    // Runs a provided TestAction. This consists of forcing the ZK node to
    // transition from the provided start_data to end_data, and checking that
    // the expected messages provided by the TestAction are received.
    //
    // If the caller is already running a resolver and would like the test case
    // to use the channel associated with that resolver, the caller can pass
    // the channel's receiver as the `external_rx` argument. Otherwise, this
    // function will run its own resolver.
    //
    pub fn run_test_case(
        &mut self,
        input: TestAction,
        external_rx: Option<&Receiver<BackendMsg>>,
    ) -> Result<(), TestError> {
        let data_path_start = self.root_path.clone() + "/state";
        let data_path_end = self.root_path.clone() + "/state";
        let start_data = input.start_data;
        let end_data = input.end_data;

        //
        // We create these early in order to appease the borrow checker, even
        // if we don't need them. We can't create them in the "None" block
        // below because we can't return a reference to a variable that goes
        // out of scope. There might be a better way to do this.
        //
        let (tx, rx) = channel();

        //
        // Start a resolver if we didn't get an external rx
        //
        let rx = match external_rx {
            Some(rx) => rx,
            None => {
                let connect_string = self.connect_string.clone();
                let root_path_resolver = self.root_path.clone();
                let log = log_from_env(DEFAULT_LOG_LEVEL)?;
                thread::spawn(move || {
                    let mut resolver = ManateePrimaryResolver::new(
                        connect_string,
                        root_path_resolver,
                        Some(log),
                    );
                    resolver.run(tx);
                });
                &rx
            }
        };

        //
        // We want this thread to not progress until the resolver_thread above
        // has time to set the watch and block. REALLY, this should be done with
        // thread priorities, but the thread-priority crate depends on some
        // pthread functions that _aren't in the libc crate for illumos_.
        // We should add these functions and upstream a change, but, for now,
        // here's this.
        //
        thread::sleep(RESOLVER_STARTUP_DELAY);

        self.set_zk_data(data_path_start, start_data)?;

        //
        // The above call to set_zk_data _might_ have caused a message to be
        // sent, depending on the node's preexisting data. We thus flush the
        // channel here.
        //
        flush_channel(rx)?;

        self.set_zk_data(data_path_end, end_data)?;

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
        for _ in 0..expected_message_count {
            let m = recv_timeout_discard_heartbeats(rx, STANDARD_RECV_TIMEOUT)?;
            received_messages.push(m);
        }

        // Make sure there are no more messages waiting
        match recv_timeout_discard_heartbeats(rx, STANDARD_RECV_TIMEOUT) {
            Err(TestError::RecvTimeout) => (),
            Err(e) => return Err(e),
            _ => return Err(TestError::UnexpectedMessage),
        }

        // Verify that the expected messages were received. Right now, we
        // intentionally don't check that they were received in a specific
        // _order_.
        //
        // TODO decide whether to enforce message ordering here and in
        // resolver_connected()
        //
        if let Some(msg) = input.added_backend {
            let msg = BackendMsg::AddedMsg(msg);
            match find_msg_match(&received_messages, &msg) {
                None => {
                    return Err(TestError::BackendMismatch(
                        "added_backend not found in received messages"
                            .to_string(),
                    ));
                }
                Some(index) => {
                    received_messages.remove(index);
                }
            }
        }
        if let Some(msg) = input.removed_backend {
            let msg = BackendMsg::RemovedMsg(msg);
            match find_msg_match(&received_messages, &msg) {
                None => {
                    return Err(TestError::BackendMismatch(
                        "removed_backend not found in received messages"
                            .to_string(),
                    ));
                }
                Some(index) => {
                    received_messages.remove(index);
                }
            }
        }

        Ok(())
    }

    //
    // Checks if a resolver is connected to zookeeper. This function assumes
    // that the "<root path>/state" node already exists, and will return 'false'
    // if it doesn't.
    //
    // This function attempts to get the resolver to send a message over the
    // passed-in channel, and then receives that message. In order to be able to
    // verify that the exact expected messages were sent, it flushes the channel
    // before changing the ZK node's data from the first value to the second
    // value. Thus, any existing messages in the channel will be discarded. Care
    // must be taken to account for this behavior when using the channel in code
    // that precedes the invocation of this function.
    //
    pub fn resolver_connected(
        &mut self,
        rx: &Receiver<BackendMsg>,
    ) -> Result<bool, TestError> {
        let data_path = self.root_path.clone() + "/state";
        let data_path_clone = data_path.clone();

        //
        //
        // Force a change in the data. If we get an error even trying to set the
        // change, we assume that zookeeper isn't running. If zookeeper isn't
        // running, then the resolver is not connected to it, so we can return
        // `false` here.
        //
        // It would be best to check that the error is the specific type we
        // expect. This is difficult, because failure::Error doesn't really let
        // you extract any structured information that we can compare to a known
        // entity.
        //
        if self
            .set_zk_data(data_path, test_data::backend_ip1_port1().raw_vec())
            .is_err()
        {
            return Ok(false);
        }

        //
        // Get rid of any outstanding messages, including the one that the above
        // call to set_zk_data might have produced
        //
        flush_channel(&rx)?;

        //
        // If we got here, it means that the _above_ call to set_zk_data()
        // succeeded, which means that zookeeper is running. Thus, we return
        // various errors below if the observed behavior doesn't make sense.
        //
        self.set_zk_data(
            data_path_clone,
            test_data::backend_ip2_port2().raw_vec(),
        )?;

        let mut received_messages = Vec::new();

        //
        // We expect to receive two messages in total. Here, we try to receive
        // the first one. If we don't get a message, then we assume that the
        // resolver is not connected.
        //
        match recv_timeout_discard_heartbeats(&rx, STANDARD_RECV_TIMEOUT) {
            Ok(m) => received_messages.push(m),
            Err(TestError::RecvTimeout) => return Ok(false),
            Err(e) => return Err(e),
        }

        //
        // We try to receive the second message. If we don't get one, we return
        // an error. We already received one message, so it's unexpected
        // behavior not to receive a second one.
        //
        match recv_timeout_discard_heartbeats(&rx, STANDARD_RECV_TIMEOUT) {
            Ok(m) => received_messages.push(m),
            Err(e) => return Err(e),
        }

        // Make sure there are no more messages waiting
        match recv_timeout_discard_heartbeats(&rx, STANDARD_RECV_TIMEOUT) {
            Err(TestError::RecvTimeout) => (),
            Err(e) => return Err(e),
            _ => return Err(TestError::UnexpectedMessage),
        }

        //
        // We now know that we have two messages. If they don't match what we
        // expect, we return an UnexpectedMessage error.
        //
        let msg = BackendMsg::RemovedMsg(
            test_data::backend_ip1_port1().removed_msg(),
        );
        match find_msg_match(&received_messages, &msg) {
            None => return Err(TestError::UnexpectedMessage),
            Some(index) => {
                received_messages.remove(index);
            }
        }
        let msg =
            BackendMsg::AddedMsg(test_data::backend_ip2_port2().added_msg());
        match find_msg_match(&received_messages, &msg) {
            None => return Err(TestError::UnexpectedMessage),
            Some(index) => {
                received_messages.remove(index);
            }
        }

        Ok(true)
    }

    //
    // A wrapper that consumes and finalizes a TestContext, runs a TestAction,
    // and panics if the test returned an error.
    //
    pub fn run_action_and_finalize(self, action: TestAction) {
        self.run_func_and_finalize(|self_ctx| {
            self_ctx.run_test_case(action, None)
        });
    }

    //
    // A wrapper that consumes and finalizes a TestContext, runs an arbitrary
    // function that takes the passed-in TestContext as an argument, and panics
    // if the function returned an error. The function can assume that it does
    // not have to handle the creation and deletion of the ZK nodes at the
    // beginning and end of the test, respectively.
    //
    pub fn run_func_and_finalize<F>(mut self, func: F)
    where
        F: FnOnce(&mut TestContext) -> Result<(), TestError>,
    {
        toggle_zookeeper(ZkStatus::Enabled)
            .expect("Failed to enable ZooKeeper");
        self.setup_zk_nodes().expect("Failed to set up ZK nodes");
        let result = func(&mut self);
        self.teardown_zk_nodes()
            .expect("Failed to tear down ZK nodes");
        self.finalize().expect("Failed to shut down tokio runtime");
        //
        // The test might have turned ZooKeeper off and not turned it back on,
        // so we do that here.
        //
        toggle_zookeeper(ZkStatus::Enabled)
            .expect("Failed to enable ZooKeeper");
        if let Err(e) = result {
            panic!("{:?}", e);
        }
    }
}

//
// Enables/disables the zookeeper server according to the passed-in `status`
// argument.
//
pub fn toggle_zookeeper(status: ZkStatus) -> Result<(), TestError> {
    const SVCADM_PATH: &str = "/usr/sbin/svcadm";
    const SVCADM_ZOOKEEPER_SERVICE_NAME: &str = "zookeeper";

    let arg = match status {
        ZkStatus::Enabled => "enable",
        ZkStatus::Disabled => "disable",
    };

    let status = Command::new(SVCADM_PATH)
        .arg(arg)
        .arg("-s")
        .arg(SVCADM_ZOOKEEPER_SERVICE_NAME)
        .status()?;

    if !status.success() {
        let msg = match status.code() {
            Some(code) => format!("svcadm exited with status {}", code),
            None => "svcadm killed by signal before finishing".to_string(),
        };
        return Err(TestError::SubprocessError(msg));
    }

    Ok(())
}

//
// Receives a message on the given channel using the given timeout, discarding
// any heartbeat messages that come over the channel
//
fn recv_timeout_discard_heartbeats(
    rx: &Receiver<BackendMsg>,
    timeout: Duration,
) -> Result<BackendMsg, TestError> {
    let start_time = Instant::now();

    //
    // The maximum granularity with which we check whether the timeout has
    // elapsed.
    //
    const GRANULARITY: Duration = Duration::from_millis(10);

    //
    // Spawn a thread to send heartbeats over its own channel at a fine-grained
    // frequency, for use in the loop below.
    //
    let (failsafe_tx, failsafe_rx) = channel();
    thread::spawn(move || loop {
        if failsafe_tx.send(BackendMsg::HeartbeatMsg).is_err() {
            break;
        }
        thread::sleep(GRANULARITY);
    });

    //
    // This is a little wonky. We'd really like to do a select() between
    // rx and failsafe_rx, but rust doesn't have a stable select() function
    // that works with mpsc channels. Bummer! Instead, while the timeout hasn't
    // run out, we try_recv on rx. If we don't find a non-hearbeat message,
    // we wait on failsafe_rx, because we _know_ a message will come over it
    // every GRANULARITY milliseconds. In this way, we rate-limit our
    // repeated calls to try_recv. Oy!
    //
    while start_time.elapsed() < timeout {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(e) => return Err(TestError::from(e)),
            Ok(msg) => match msg {
                BackendMsg::HeartbeatMsg => (),
                msg => return Ok(msg),
            },
        }
        failsafe_rx.recv()?;
    }

    Err(TestError::RecvTimeout)
}

//
// Flushes the provided Receiver of any non-heartbeat messages
//
fn flush_channel(rx: &Receiver<BackendMsg>) -> Result<(), TestError> {
    loop {
        match recv_timeout_discard_heartbeats(&rx, STANDARD_RECV_TIMEOUT) {
            Ok(_) => {
                continue;
            }
            Err(TestError::RecvTimeout) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

///
/// Given a list of BackendMsg and a BackendMsg to find, returns the index of
/// the desired item in the list, or None if the item is not in the list. We
/// return the index, rather than the item itself, to allow callers of the
/// function to more easily manipulate the list afterward.
///
pub fn find_msg_match(
    list: &[BackendMsg],
    to_find: &BackendMsg,
) -> Option<usize> {
    for (index, item) in list.iter().enumerate() {
        if item == to_find {
            return Some(index);
        }
    }
    None
}

//
// Represents the input and expected output for a single test case.
//
pub struct TestAction {
    // Data for a given zookeeper node
    pub start_data: Vec<u8>,
    // Another data string for the same node
    pub end_data: Vec<u8>,
    //
    // The messages expected when transitioning from start_data to end_data
    //
    pub added_backend: Option<BackendAddedMsg>,
    pub removed_backend: Option<BackendRemovedMsg>,
}

pub fn parse_log_level(s: String) -> Result<Level, TestError> {
    match s.to_lowercase().as_str() {
        "trace" => Ok(Level::Trace),
        "debug" => Ok(Level::Debug),
        "info" => Ok(Level::Info),
        "warning" => Ok(Level::Warning),
        "error" => Ok(Level::Error),
        "critical" => Ok(Level::Critical),
        _ => Err(TestError::InvalidLogLevel),
    }
}

pub fn log_level_from_env() -> Result<Option<Level>, TestError> {
    let level_env = env::var_os(LOG_LEVEL_ENV_VAR);
    let level = match level_env {
        Some(level_str) => Some(parse_log_level(
            level_str
                .into_string()
                .expect("Log level string has invalid Unicode data"),
        )?),
        None => None,
    };
    Ok(level)
}

pub fn standard_log(l: Level) -> Logger {
    Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::with_name(crate_name!(), std::io::stdout()).build(),
            l,
        ))
        .fuse(),
        o!("build-id" => crate_version!()),
    )
}

pub fn log_from_env(default_level: Level) -> Result<Logger, TestError> {
    let level = log_level_from_env()?.unwrap_or(default_level);
    Ok(standard_log(level))
}
