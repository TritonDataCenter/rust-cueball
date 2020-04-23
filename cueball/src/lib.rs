// Copyright 2020 Joyent, Inc.

//! A multi-node service connection pool
//!
//! Cueball is a library for "playing pool" -- managing a pool of connections to
//! a multi-node service. This implementation of cueball is inspired by the
//! original Node.js implementation of
//! [cueball](https://joyent.github.io/node-cueball/) that is used by many of
//! Joyent's services and software components. The rust implementation relies on
//! two primary traits in order to manage a set of connections across a set
//! nodes providing a service. These are the
//! [`Resolver`](resolver/trait.Resolver.html) trait and the
//! [`Connection`](connection/trait.Connection.html) trait.
//!
//! ## Resolvers
//!
//! A *resolver* is responsible for locating all of the nodes or backends
//! available within a logical service, obtaining their IP address and port
//! information (or whatever is required to connect to them) and tracking
//! them. This is normally a service discovery client of some form. An example
//! of this would be a DNS-based Resolver implementation that uses DNS SRV
//! records as a form of service discovery mechanism to find backends.
//!
//! ## Connections
//!
//! In cueball, a *connection* is not necessarily just a TCP socket. It can be
//! anything that provides some kind of logical connection to a service, as long
//! as it obeys a similar interface to a socket.
//!
//! This is intended to allow users of the API to represent a "connection" as an
//! application or session layer concept. For example, it could be useful to
//! construct a pool of connections to an LDAP server that perform a bind
//! operation (authenticate) before they are considered *connected*.
//!
//! In addition to a [`Resolver`](resolver/trait.Resolver.html) and
//! [`Connection`](connection/trait.Connection.html) implementation cueball
//! users also provide the cueball connection pool with a function to establish
//! a *connection* to the desired service. The trait bounds established by the
//! cueball connection pool for this function are as follows:
//! ```rust.ignore
//! FnMut(&Backend) -> C + Send + 'static
//! where C: Connection
//! ```
//! The requirement is a function that takes a reference to a
//! [`Backend`](backend/struct.Backend.html) from a resolver and returns some
//! instance of a [`Connection`](connection/trait.Connection.html).
//!
//! The purpose of this function is to provide a way to capture application
//! level configuration information required to establish a *connection* to a
//! service. *e.g.* A database connection might require application-specific
//! configuration such as a database name or user name in order to establish a
//! connection.
//!
//! ## Rebalancing
//!
//! As [`Backend`](backend/struct.Backend.html)s for a service come and go the
//! connection pool rebalances the configured number of connections
//! (`max_connections`) across the available set of
//! [`Backend`](backend/struct.Backend.html)s. Rebalancing occurs when a
//! [`Resolver`](resolver/trait.Resolver.html) notifies the connection pool that
//! a new backend has been added or that an existing backend has been
//! removed. The connection pool rebalances the connections in response to one
//! of these events in order to maintain an even distribution of the connections
//! among the available backends.
//!
//! The connection pool uses a configurable delay when a message is received
//! from the [`Resolver`](resolver/trait.Resolver.html) prior to performing the
//! actual rebalancing. This delay is to account for situations where multiple
//! messages might be sent by the [`Resolver`](resolver/trait.Resolver.html) in
//! a very short span of time and allows the connection pool to be more
//! efficient in rebalancing the connections. The default rebalancing delay time
//! is 100 milliseconds.
//!
//! Rebalancing can cause the connection pool to temporarily exceed the maximum
//! number of connections configured for the pool. If the
//! [`Resolver`](resolver/trait.Resolver.html) notifies the connection pool that
//! a backend is removed, but connections for that backend are still in use the
//! connection count may exceed the maximum until those connections are returned
//! to the connection pool and discarded.
//!
//! ## Decoherence
//!
//! Decoherence in cueball is used to mean a periodic random shuffling of the order
//! of connections in the connection pool. The goal of decoherence is to avoid
//! undesirable patterns that could emerge in the lifetime of the connection
//! pool. For example suppose that a service has three backends, `A`, `B`, and
//! `C` and the connection pool has a maximum connection count of nine. The
//! initial connection distribution might look as follows:
//!
//! | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
//! |---|---|---|---|---|---|---|---|---|
//! | A | B | C | A | B | C | A | B | C |
//!
//! The cueball connection pool uses a queue internally to store the
//! connections. Given that connections from the pool may be claimed for
//! nonuniform periods of time it is possible that the queue could arrive at the
//! following state from its initial state:
//!
//! | 1 | 4 | 7 | 2 | 5 | 8 | 3 | 6 | 9 |
//! |---|---|---|---|---|---|---|---|---|
//! | A | A | A | B | B | B | C | C | C |
//!
//! This situation is not ideal because the same backend must handle multiple
//! consecutive requests while the other backends are idle. The ideal for
//! cueball is to have an even distribution of work among the backends not just
//! with respect to connection count, but also with respect to to request
//! distribution over time. Now admittedly the above example is an extreme
//! case and the pattern could quickly resolve itself based on the workload, but
//! there is no guarantee that would be the case. The goal of the periodic
//! decoherence shuffle in cueball is to disrupt these sorts of patterns that
//! might arise and persist for an extended period.
//!
//! There is one configuration options related to decoherence:
//! `decoherence_interval`. The `decoherence_interval` represents the length of
//! the period of the decoherence shuffle in seconds. If no value is specified
//! for this in the
//! [`ConnectionPoolOptions`](connection_pool/types/struct.ConnectionPoolOptions.html)
//! struct the default value is 300 seconds.
//!
//! ## Example
//!
//! Use of cueball for connection management requires both an implementation of
//! the [`Resolver`](resolver/trait.Resolver.html) trait and an implementation
//! of the [`Connection`](connection/trait.Connection.html) trait. Implementers
//! of the [`Resolver`](resolver/trait.Resolver.html) trait provide information
//! to the connection pool about the nodes availble to provide a given
//! service. The [`Connection`](connection/trait.Connection.html) trait defines
//! a behavior for establishing and closing a *connection* to a particular
//! service.
//!
//! Here is an example that uses a hypothetical
//! [`Resolver`](resolver/trait.Resolver.html) and
//! [`Connection`](connection/trait.Connection.html) implementation to create a
//! cueball connection pool.
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
//! let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
//! let log = Logger::root(
//!     Mutex::new(
//!         slog_term::FullFormat::new(plain).build()
//!     ).fuse(),
//!     o!("build-id" => "0.1.0")
//! );
//!
//! let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);
//! let be2 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55556);
//! let be3 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55557);
//!
//! let resolver = FakeResolver::new(vec![be1, be2, be3]);
//!
//! let pool_opts = ConnectionPoolOptions::<FakeResolver> {
//!     max_connections: 15,
//!     claim_timeout: Some(1000)
//!     resolver: resolver,
//!     log: log.clone(),
//!     decoherence_interval: None,
//! };
//!
//! let pool = ConnectionPool::<DummyConnection, FakeResolver>::new(pool_opts);
//!
//! for _ in 0..10 {
//!     let pool = pool.clone();
//!     thread::spawn(move || {
//!         let conn = pool.claim()?;
//!         // Do stuff here
//!         // The connection is returned to the pool when it falls out of scope.
//!     })
//! }
//! ```
//!
//! There are several implementations of the
//! [`Resolver`](resolver/trait.Resolver.html) and
//! [`Connection`](connection/trait.Connection.html) traits that may be useful
//! to anyone looking to get started with `cueball`:
//!
//! ## [`Resolver`](resolver/trait.Resolver.html) trait implementer
//!
//! * [`cueball-static-resolver`](https://github.com/joyent/rust-cueball-static-resolver)
//! * [`cueball-dns-resolver`](https://github.com/joyent/rust-cueball-dns-resolver)
//!
//! ## [`Connection`](connection/trait.Connection.html) trait implementer
//!
//! * [`cueball-tcp-stream-connection`](https://github.com/joyent/rust-cueball-tcp-stream-connection)
//! * [`cueball-postgres-connection`](https://github.com/joyent/rust-cueball-postgres-connection)

#![allow(missing_docs)]

pub mod backend;
pub mod connection;
pub mod connection_pool;
pub mod error;
pub mod resolver;
