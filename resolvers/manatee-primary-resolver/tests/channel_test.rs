//
// Copyright 2020 Joyent, Inc
//

use std::str::FromStr;
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use cueball::resolver::Resolver;
use uuid::Uuid;

use common::util::{
    self, DEFAULT_LOG_LEVEL, RESOLVER_STARTUP_DELAY, SLACK_DURATION,
    STANDARD_RECV_TIMEOUT,
};
use cueball_manatee_primary_resolver::{
    common, ManateePrimaryResolver, ZkConnectString, HEARTBEAT_INTERVAL,
};

//
// Tests that the resolver exits immediately if the receiver is closed when the
// resolver starts
//
#[test]
fn channel_test_start_with_closed_rx() {
    let (tx, rx) = channel();

    let connect_string = ZkConnectString::from_str("10.77.77.6:2181").unwrap();
    let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
    let log = util::log_from_env(DEFAULT_LOG_LEVEL).unwrap();

    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    // Close the receiver before the resolver even starts
    drop(rx);

    // Run the resolver and signal the condvar when it exits
    thread::spawn(move || {
        let (lock, cvar) = &*pair2;
        let mut resolver =
            ManateePrimaryResolver::new(connect_string, root_path, Some(log));
        resolver.run(tx);
        let mut resolver_exited = lock.lock().unwrap();
        *resolver_exited = true;
        cvar.notify_all();
    });

    let (lock, cvar) = &*pair;

    // Wait for the resolver to exit, allowing a brief timeout
    let mut resolver_exited = lock.lock().unwrap();
    while !*resolver_exited {
        let result = cvar
            .wait_timeout(resolver_exited, STANDARD_RECV_TIMEOUT)
            .unwrap();
        resolver_exited = result.0;
        if result.1.timed_out() {
            panic!(
                "Resolver did not immediately exit upon starting with \
                 closed receiver"
            );
        }
    }
}

//
// Tests that the resolver exits within HEARTBEAT_INTERVAL if the receiver is
// closed while the resolver is running.
//
#[test]
fn channel_test_exit_upon_closed_rx() {
    let (tx, rx) = channel();

    let connect_string = ZkConnectString::from_str("127.0.0.1:2181").unwrap();
    let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
    let log = util::log_from_env(DEFAULT_LOG_LEVEL).unwrap();

    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    // Start the resolver
    thread::spawn(move || {
        let (lock, cvar) = &*pair2;
        let mut resolver =
            ManateePrimaryResolver::new(connect_string, root_path, Some(log));
        resolver.run(tx);
        let mut resolver_exited = lock.lock().unwrap();
        *resolver_exited = true;
        cvar.notify_all();
    });

    // See comment in TestContext::run_test_case about why we sleep here.
    thread::sleep(RESOLVER_STARTUP_DELAY);

    // Close the receiver once the resolver has started
    drop(rx);

    let (lock, cvar) = &*pair;

    // Wait for the resolver to exit
    let mut resolver_exited = lock.lock().unwrap();
    while !*resolver_exited {
        //
        // We should wait a little longer than HEARTBEAT_INTERVAL for the
        // resolver to notice that the receiver got closed.
        //
        let result = cvar
            .wait_timeout(resolver_exited, SLACK_DURATION + HEARTBEAT_INTERVAL)
            .unwrap();
        resolver_exited = result.0;
        if result.1.timed_out() {
            panic!("Resolver did not exit upon closure of receiver");
        }
    }
}
