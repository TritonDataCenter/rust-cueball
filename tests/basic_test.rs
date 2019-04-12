use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use slog::{o, Drain, Logger};

use cueball::backend;
use cueball::backend::{Backend, BackendAddress, BackendPort};
use cueball::connection::Connection;
use cueball::connection_pool::types::{ConnectionCount, ConnectionPoolOptions};
use cueball::connection_pool::ConnectionPool;
use cueball::error::Error;
use cueball::resolver::{BackendAddedMsg, BackendMsg, Resolver};

#[derive(Debug)]
pub struct DummyConnection {
    addr: SocketAddr,
    connected: bool
}

impl Connection for DummyConnection {
    fn new(b: &Backend) -> Self {
        let addr = SocketAddr::from((b.address, b.port));

        DummyConnection {
            addr: addr,
            connected: false
        }
    }

    fn connect(&mut self) -> Result<(), Error> {
        self.connected = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        self.connected = false;
        Ok(())
    }
}

pub struct FakeResolver {
    backends: Vec<(BackendAddress, BackendPort)>,
    pool_tx: Option<Sender<BackendMsg>>,
    error: Option<Error>,
    started: bool
}

impl FakeResolver {
    pub fn new(backends: Vec<(BackendAddress, BackendPort)>) -> Self {
        FakeResolver {
            backends: backends,
            pool_tx: None,
            error: None,
            started: false
        }
    }
}

impl Resolver for FakeResolver {
    fn start(&mut self, s: Sender<BackendMsg>) {
        if !self.started {
            self.backends.iter().for_each(|b| {
                let backend = Backend::new(&b.0, b.1);
                let backend_key = backend::srv_key(&backend);
                let backend_msg = BackendMsg::AddedMsg(BackendAddedMsg {
                    key: backend_key,
                    backend: backend
                });
                s.send(backend_msg).unwrap();
            });
            self.pool_tx = Some(s);
            self.started = true;
        }
    }

    fn stop(&mut self) {
        self.started = false;
        ()
    }

    fn get_last_error(&self) -> Option<String> {
        if let Some(err) = &self.error {
            let err_str = format!("{}", err);
            Some(err_str)
        } else {
            None
        }
    }
}

#[test]
fn connection_pool_claim() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0")
    );

    // Only use one backend to keep the test deterministic. Cueball allows for
    // some slop in the maximum number of pool connections as new backends come
    // online and connections are reblanced and having multiple backends that
    // start asynchronously would make it difficult for the test to be reliable.
    let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);

    let resolver = FakeResolver::new(vec![be1]);

    let pool_opts = ConnectionPoolOptions::<FakeResolver> {
        maximum: 3,
        claim_timeout: Some(1000),
        resolver: resolver,
        log: log.clone(),
        rebalancer_action_delay: None
    };

    let max_connections = pool_opts.maximum.clone();

    let pool = ConnectionPool::<DummyConnection, FakeResolver>::new(pool_opts);

    // Wait for total_connections to reach the maximum
    let mut all_conns_established = false;
    while !all_conns_established {
        if let Some(stats) = pool.get_stats() {
            if stats.total_connections == max_connections.into() {
                all_conns_established = true;
            }
        }
    }

    let barrier1 = Arc::new(Barrier::new(4));
    let barrier2 = Arc::new(Barrier::new(4));

    let barrier1_clone1 = barrier1.clone();
    let barrier2_clone1 = barrier2.clone();
    let pool_clone1 = pool.clone();
    let thread1 = thread::spawn(move || {
        let conn_result = pool_clone1.claim();
        assert!(conn_result.is_ok());
        barrier1_clone1.wait();
        barrier2_clone1.wait();
    });

    let barrier1_clone2 = barrier1.clone();
    let barrier2_clone2 = barrier2.clone();
    let pool_clone2 = pool.clone();
    let thread2 = thread::spawn(move || {
        let conn_result = pool_clone2.claim();
        assert!(conn_result.is_ok());
        barrier1_clone2.wait();
        barrier2_clone2.wait();
    });

    let barrier1_clone3 = barrier1.clone();
    let barrier2_clone3 = barrier2.clone();
    let pool_clone3 = pool.clone();
    let thread3 = thread::spawn(move || {
        let conn_result = pool_clone3.claim();
        assert!(conn_result.is_ok());
        barrier1_clone3.wait();
        barrier2_clone3.wait();
    });

    barrier1.wait();

    let m_claim1 = pool.try_claim();
    assert!(m_claim1.is_none());

    // This will timeout after one second based on the claim_timeout specfied in
    // the pool options
    let m_claim2 = pool.claim();
    assert!(m_claim2.is_err());

    barrier2.wait();

    let _ = thread1.join();
    let _ = thread2.join();
    let _ = thread3.join();

    let m_claim3 = pool.try_claim();
    assert!(m_claim3.is_some());
}

#[test]
fn connection_pool_stop() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0")
    );

    // Only use one backend to keep the test deterministic. Cueball allows for
    // some slop in the maximum number of pool connections as new backends come
    // online and connections are reblanced and having multiple backends that
    // start asynchronously would make it difficult for the test to be reliable.
    let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);

    let resolver = FakeResolver::new(vec![be1]);

    let pool_opts = ConnectionPoolOptions::<FakeResolver> {
        maximum: 3,
        claim_timeout: Some(1000),
        resolver: resolver,
        log: log.clone(),
        rebalancer_action_delay: None
    };

    let max_connections = pool_opts.maximum.clone();

    let mut pool = ConnectionPool::<DummyConnection, FakeResolver>::new(pool_opts);

    // Wait for total_connections to reach the maximum
    let mut all_conns_established = false;
    while !all_conns_established {
        if let Some(stats) = pool.get_stats() {
            if stats.total_connections == max_connections.into() {
                all_conns_established = true;
            }
        }
    }

    let stop_result = pool.stop();
    assert!(stop_result.is_ok());
}

// TODO: Use quickcheck for this test. At very least the max_connections count
// could be easily generated.
#[test]
fn connection_pool_accounting() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0")
    );

    // Only use one backend to keep the test deterministic. Cueball allows for
    // some slop in the maximum number of pool connections as new backends come
    // online and connections are reblanced and having multiple backends that
    // start asynchronously would make it difficult for the test to be reliable.
    let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);

    let resolver = FakeResolver::new(vec![be1]);

    let pool_opts = ConnectionPoolOptions::<FakeResolver> {
        maximum: 3,
        claim_timeout: Some(1000),
        resolver: resolver,
        log: log.clone(),
        rebalancer_action_delay: None
    };

    let max_connections: ConnectionCount = pool_opts.maximum.clone().into();

    let mut pool = ConnectionPool::<DummyConnection, FakeResolver>::new(pool_opts);

    // Wait for total_connections to reach the maximum
    let mut all_conns_established = false;
    while !all_conns_established {
        if let Some(stats) = pool.get_stats() {
            if stats.total_connections == max_connections {
                all_conns_established = true;
            }
        }
    }

    // Sanity check our starting stats
    let m_starting_stats = pool.get_stats();
    assert!(m_starting_stats.is_some());
    let starting_stats = m_starting_stats.unwrap();
    assert_eq!(starting_stats.total_connections, max_connections);
    assert_eq!(starting_stats.idle_connections, max_connections);
    assert_eq!(starting_stats.pending_connections, 0.into());

    let conn_result1 = pool.claim();
    assert!(conn_result1.is_ok());

    let m_stats_check1 = pool.get_stats();
    assert!(m_stats_check1.is_some());
    let stats_check1 = m_stats_check1.unwrap();
    assert_eq!(stats_check1.total_connections, max_connections);
    assert_eq!(stats_check1.idle_connections, max_connections - 1.into());
    assert_eq!(stats_check1.pending_connections, 0.into());

    let conn_result2 = pool.claim();
    assert!(conn_result2.is_ok());

    let m_stats_check2 = pool.get_stats();
    assert!(m_stats_check2.is_some());
    let stats_check2 = m_stats_check2.unwrap();
    assert_eq!(stats_check2.total_connections, max_connections);
    assert_eq!(stats_check2.idle_connections, max_connections - 2.into());
    assert_eq!(stats_check2.pending_connections, 0.into());

    let conn_result3 = pool.claim();
    assert!(conn_result3.is_ok());

    let m_stats_check3 = pool.get_stats();
    assert!(m_stats_check3.is_some());
    let stats_check3 = m_stats_check3.unwrap();
    assert_eq!(stats_check3.total_connections, max_connections);
    assert_eq!(stats_check3.idle_connections, max_connections - 3.into());
    assert_eq!(stats_check3.pending_connections, 0.into());

    drop(conn_result3);

    let m_stats_check4 = pool.get_stats();
    assert!(m_stats_check4.is_some());
    let stats_check4 = m_stats_check4.unwrap();
    assert_eq!(stats_check4.total_connections, max_connections);
    assert_eq!(stats_check4.idle_connections, max_connections - 2.into());
    assert_eq!(stats_check4.pending_connections, 0.into());

    drop(conn_result2);

    let m_stats_check5 = pool.get_stats();
    assert!(m_stats_check5.is_some());
    let stats_check5 = m_stats_check5.unwrap();
    assert_eq!(stats_check5.total_connections, max_connections);
    assert_eq!(stats_check5.idle_connections, max_connections - 1.into());
    assert_eq!(stats_check5.pending_connections, 0.into());

    drop(conn_result1);

    let m_stats_check6 = pool.get_stats();
    assert!(m_stats_check6.is_some());
    let stats_check6 = m_stats_check6.unwrap();
    assert_eq!(stats_check6.total_connections, max_connections);
    assert_eq!(stats_check6.idle_connections, max_connections);
    assert_eq!(stats_check6.pending_connections, 0.into());

    let stop_result = pool.stop();
    assert!(stop_result.is_ok());

    let m_stats_check7 = pool.get_stats();
    assert!(m_stats_check7.is_none());
    assert_eq!(pool.get_state(), String::from("stopped"));
}
