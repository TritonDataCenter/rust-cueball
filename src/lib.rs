/*
 * Copyright 2019 Joyent, Inc.
 */

//! cueball is a library for "playing pool" -- managing a pool of connections to
//! a multi-node service. Information about the available service nodes to
//! connect to is provided by implementors of the Resolver trait.

pub mod backend;
pub mod connection;
#[allow(dead_code)] // TODO: Remove after initial dev
pub mod connection_pool;
pub mod error;
pub mod resolver;
