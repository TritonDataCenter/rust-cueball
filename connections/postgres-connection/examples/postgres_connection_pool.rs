/*
 * Copyright 2019 Joyent, Inc.
 */

//! A basic example that demonstrates using the StaticIpResolver for cueball to
//! establish a basic connection pool of PostgreSQL connections.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Mutex;

use slog::{o, Drain, Logger};

use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball_postgres_connection::{
    PostgresConnection,
    PostgresConnectionConfig,
    TlsConfig
};
use cueball_static_resolver::StaticIpResolver;

fn main() {
    let be1 = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5432);

    let resolver = StaticIpResolver::new(vec![be1]);

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let user = "postgres";
    let database = "test";
    let application_name = "postgres-connection-pool";
    let pg_config = PostgresConnectionConfig {
        user: Some(user.into()),
        password: None,
        host: None,
        port: None,
        database: Some(database.into()),
        application_name: Some(application_name.into()),
        tls_config: TlsConfig::disable()
    };
    let connection_creator = PostgresConnection::connection_creator(pg_config);
    let pool_opts = ConnectionPoolOptions {
        max_connections: Some(5),
        claim_timeout: None,
        log: Some(log),
        rebalancer_action_delay: None,
        decoherence_interval: None
    };

    let _pool = ConnectionPool::new(pool_opts, resolver, connection_creator);

    println!("Cueball!");

    loop {}
}
