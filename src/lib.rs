//
// Copyright 2019 Joyent, Inc.
//

//! A multi-node service connection pool
//!
//! Cueball is a library for "playing pool" -- managing a pool of connections to
//! a multi-node service. Use of cueball for connection management requires both
//! an implementation of the [`Resolver`]: trait.Resolver.html trait and an implementation of the
//! [`Connection`]: trait.Connection.html trait. Implementors of the `Resolver` trait provide information
//! to the connection pool about the nodes availble to provide a given
//! service. The `Connection` trait defines a behavior for establishing and
//! closing a *connection* to a particular service.
//!
//! # Example
//!
//! Use a hypothetical `Resolver` and `Connection` image to create a cueball
//! connection pool.
//!
//! ```rust,ignore
//! use std::thread;
//! use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//! use std::sync::{Arc, Barrier, Mutex};
//! use std::sync::mpsc::Sender;
//! use std::{thread, time};
//!
//! use slog::{Drain, Logger, info, o};
//!
//! use cueball::backend;
//! use cueball::backend::{Backend, BackendAddress, BackendPort};
//! use cueball::connection::Connection;
//! use cueball::connection_pool::ConnectionPool;
//! use cueball::connection_pool::types::ConnectionPoolOptions;
//! use cueball::error::Error;
//! use cueball::resolver::{BackendAddedMsg, BackendMsg, Resolver};
//!
//! fn main() {
//!     let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
//!     let log = Logger::root(
//!         Mutex::new(
//!             slog_term::FullFormat::new(plain).build()
//!         ).fuse(),
//!         o!("build-id" => "0.1.0")
//!     );
//!
//!     let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);
//!     let be2 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55556);
//!     let be3 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55557);
//!
//!     let resolver = FakeResolver::new(vec![be1, be2, be3]);
//!
//!     let pool_opts = ConnectionPoolOptions::<FakeResolver> {
//!         maximum: 10,
//!         claim_timeout: Some(1000)
//!         resolver: resolver,
//!         log: log.clone()
//!     };
//!
//!     let pool = ConnectionPool::<DummyConnection, FakeResolver>::new(pool_opts);
//!
//!     for _ in 0..10 {
//!         let pool = pool.clone();
//!         thread::spawn(move || {
//!             let conn = pool.claim()?;
//!             // Do stuff here
//!             // The connection is returned to the pool when it falls out of scope.
//!         })
//!     }
//! }
//! ```

#![allow(missing_docs)]

pub mod backend;
pub mod connection;
pub mod connection_pool;
pub mod error;
pub mod resolver;
