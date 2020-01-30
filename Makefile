#
# Copyright 2020 Joyent, Inc
#

#
# Variables
#

NAME = rust-cueball
CARGO ?= cargo
RUST_CLIPPY_ARGS ?= -- -D clippy::all
RUSTFMT_ARGS ?= -- --check

#
# Repo-specific targets
#
.PHONY: all
all: build-cueball

.PHONY: build-cueball
build-cueball:
	$(CARGO) build --release

.PHONY: test
test:
	$(CARGO) test

.PHONY: test-unit
test-unit:
	$(CARGO) test --lib

.PHONY: check
check:
	$(CARGO) clean && $(CARGO) clippy $(RUST_CLIPPY_ARGS)

.PHONY: fmtcheck
fmtcheck:
	$(CARGO) fmt $(RUSTFMT_ARGS)
