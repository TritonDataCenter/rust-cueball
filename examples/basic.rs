use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Barrier, Mutex};
use std::sync::mpsc::Sender;
use std::{thread, time};

use slog::{Drain, Logger, info, o};

use cueball::backend;
use cueball::backend::{Backend, BackendAddress, BackendPort};
use cueball::connection::Connection;
use cueball::connection_pool::ConnectionPool;
use cueball::connection_pool::types::ConnectionPoolOptions;
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

    fn set_unwanted(&self) {
        ()
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
                let backend = Backend::new(&b.0, &b.1);
                let backend_key = backend::srv_key(&backend);
                let backend_msg =
                    BackendMsg::AddedMsg(
                        BackendAddedMsg {
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

fn main() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(
            slog_term::FullFormat::new(plain).build()
        ).fuse(),
        o!("build-id" => "0.1.0")
    );

    info!(log, "running basic cueball example");

    // Start a pool and start some threads to use connections
    let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);
    let be2 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55556);
    let be3 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55557);

    let resolver = FakeResolver::new(vec![be1, be2, be3]);

    let pool_opts = ConnectionPoolOptions::<FakeResolver> {
        domain: String::from("abc.com"),
        spares: 3,
        maximum: 3,
        service: None,
        claim_timeout: Some(1000),
        resolver: resolver,
        log: log.clone()
    };

    let pool = ConnectionPool::<DummyConnection, FakeResolver>::new(pool_opts);

    // Backend initialization happens asynchronously so give the backends some
    // time to get started
    let sleep_time = time::Duration::from_millis(1000);
    thread::sleep(sleep_time);

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

    let _  = thread1.join();
    let _  = thread2.join();
    let _  = thread3.join();

    let m_claim3 = pool.try_claim();
    assert!(m_claim3.is_some());
}
