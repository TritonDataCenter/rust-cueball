# manatee-echo-resolver

The `manatee-echo-resolver` runs a manatee primary resolver and prints its
logging, as well as messages received over the cueball channel, to standard out.
This is useful for ad-hoc testing. Here's how to build and run:

```
# Note that these paths are relative to the repository root
$ cd rust-cueball

$ cargo build

$ ./target/debug/echo-resolver --help
```
