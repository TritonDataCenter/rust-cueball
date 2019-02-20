/*
 * Copyright 2019 Joyent, Inc.
 */

use std::vec::Vec;

use crate::connection::Connection;
use crate::error::Error;
use crate::resolver::Resolver;


pub struct ConnectionPoolStats {
    total_connections: u32,
    idle_connections: u32,
    pending_connections: u32
}

pub struct ConnectionPoolOptions<R> {
    domain: String,
    spares: u32,
    maximum: u32,
    service: Option<String>,
    target_claim_delay: Option<u32>,
    resolver: Option<R>
}

pub struct ConnectionPool<C, R> {
    connections: Vec<C>,
    last_error: Option<Error>,
    resolver: R,
    stats: ConnectionPoolStats,
    domain: String,
    spares: u32,
    maximum: u32,
    service: Option<String>,
    target_claim_delay: u32
}

impl<C, R> ConnectionPool<C, R>
where
    C: Connection,
    R: Resolver
{
    pub fn new(o: ConnectionPoolOptions<R>) -> Self {
        std::unimplemented!()
    }

    pub fn stop() -> () {
        std::unimplemented!()
    }

    pub fn claim() -> Result<C, Error> {
        std::unimplemented!()
    }

    pub fn try_claim() -> Option<C> {
        std::unimplemented!()
    }

    pub fn get_last_error() -> Option<String> {
        std::unimplemented!()
    }

    pub fn get_stats() -> Option<ConnectionPoolStats> {
        std::unimplemented!()
    }
}
