# cueball-manatee-primary-resolver

## About

This is an implementation of the
[cueball](https://github.com/joyent/rust-cueball) `Resolver` trait that provides
a list of backends for use by the connection pool. See the cueball documentation
for more information.

This resolver is specific to the Joyent
[manatee](https://github.com/joyent/manatee) project. It queries a zookeeper
cluster to determine the PostgreSQL replication primary from a set of PostgreSQL
replication peers.

## Useful tools

The `manatee-echo-resolver` binary, in `/rust-cueball/tools/`, runs a manatee
primary resolver and prints its logging, as well as messages received over the
cueball channel, to standard out. This is useful for ad-hoc testing. Here's how
to build and run:

```
# Note that these paths are relative to the repository root
$ cd rust-cueball

$ cargo build

$ ./target/debug/echo-resolver --help
```

## Test suite

### Prerequisites

The test suite requires a running ZooKeeper instance with the following
properties:
* ZooKeeper is accessible on localhost at port 2181.
* ZooKeeper is running as an [SMF service](https://wiki.smartos.org/basic-smf-commands/).

The easiest way to meet both of these criteria is to run the test suite on
Joyent SmartOS and install ZooKeeper in your test zone via
[pkgsrc](https://pkgsrc.joyent.com/).

The ZooKeeper instance does not require any other setup ahead of time -- the
test suite will handle the creation and deletion of test nodes with mock data.

### Running the test suite

To run the test suite, run `cargo test`.

The amount of log output may be distracting. You can quell this by setting the
`RESOLVER_LOG_LEVEL` to something high, perhaps `critical`. The default log
level is `info`. If you do want to look at the log output, you may want to pipe
it into [bunyan](https://github.com/trentm/node-bunyan) for readability. Because
the tests run in parallel where it is safe to do so, their log output will
occasionally interleave, rendering it unparseable. If this is a nuisance, you
can run the tests consecutively using the instructions
[here](https://doc.rust-lang.org/book/ch11-02-running-tests.html).
