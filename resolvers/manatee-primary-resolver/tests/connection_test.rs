//
// Copyright 2020 Joyent, Inc
//

#[cfg(target_os = "solaris")]
use std::sync::mpsc::channel;
#[cfg(target_os = "solaris")]
use std::thread;

#[cfg(target_os = "solaris")]
use serial_test::serial;

#[cfg(target_os = "solaris")]
use cueball::resolver::Resolver;

#[cfg(target_os = "solaris")]
use common::util::{
    self, TestContext, TestError, ZkStatus, RESOLVER_STARTUP_DELAY,
};
#[cfg(target_os = "solaris")]
use cueball_manatee_primary_resolver::{
    common, ManateePrimaryResolver, MAX_BACKOFF_INTERVAL,
};

#[cfg(target_os = "solaris")]
#[test]
#[serial]
fn connection_test_start_with_unreachable_zookeeper() {
    TestContext::default().run_func_and_finalize(|ctx| {
        let (tx, rx) = channel();

        let connect_string_resolver = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();
        let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();

        // Turn ZooKeeper off
        util::toggle_zookeeper(ZkStatus::Disabled)?;

        // We expect resolver not to connect at this point
        thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(
                connect_string_resolver,
                root_path_resolver,
                Some(log),
            );
            resolver.run(tx);
        });

        // See comment in TestContext::run_test_case about why we sleep here.
        thread::sleep(RESOLVER_STARTUP_DELAY);

        let connected = ctx.resolver_connected(&rx)?;
        if connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should not be connected \
                 (is ZooKeeper running, somehow?)"
                    .to_string(),
            ));
        }

        // Turn ZooKeeper back on
        util::toggle_zookeeper(ZkStatus::Enabled)?;

        //
        // Wait the maximum possible amount of time that could elapse without
        // the resolver reconnecting
        //
        thread::sleep(*MAX_BACKOFF_INTERVAL);

        // Check that the resolver has reconnected
        let connected = ctx.resolver_connected(&rx)?;
        if !connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should be connected to ZooKeeper".to_string(),
            ));
        }
        Ok(())
    });
}

#[cfg(target_os = "solaris")]
#[test]
#[serial]
fn connection_test_reconnect_after_zk_hiccup() {
    TestContext::default().run_func_and_finalize(|ctx| {
        util::toggle_zookeeper(ZkStatus::Enabled)?;

        let (tx, rx) = channel();

        let connect_string_resolver = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();
        let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();

        thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(
                connect_string_resolver,
                root_path_resolver,
                Some(log),
            );
            resolver.run(tx);
        });

        // See comment in TestContext::run_test_case about why we sleep here.
        thread::sleep(RESOLVER_STARTUP_DELAY);

        let connected = ctx.resolver_connected(&rx)?;
        if !connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should be connected to ZooKeeper".to_string(),
            ));
        }

        // Turn ZooKeeper off
        util::toggle_zookeeper(ZkStatus::Disabled)?;

        let connected = ctx.resolver_connected(&rx)?;
        if connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should not be connected \
                 (is ZooKeeper running, somehow?)"
                    .to_string(),
            ));
        }

        // Turn ZooKeeper back on
        util::toggle_zookeeper(ZkStatus::Enabled)?;

        //
        // Wait the maximum possible amount of time that could elapse without
        // the resolver reconnecting, plus a little extra to be safe.
        //
        thread::sleep(*MAX_BACKOFF_INTERVAL);

        // Check that the resolver has reconnected
        let connected = ctx.resolver_connected(&rx)?;
        if !connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should be connected to ZooKeeper".to_string(),
            ));
        }
        Ok(())
    });
}
