// Copyright 2019 Joyent, Inc.

use std::net::IpAddr;

use base64;
use sha1::Sha1;

use derive_more::{Display, From, Into};

/// A base64 encoded identifier based on the backend name, address, and port.
#[derive(
    Clone, Debug, Display, Eq, From, Hash, Into, Ord, PartialOrd, PartialEq,
)]
pub struct BackendKey(String);
/// The port number for a backend. This is a type alias for u16.
pub type BackendPort = u16;
/// The concatenation of the backend address and port with a colon
/// delimiter. This is a type alias for String.
pub type BackendName = String;
/// The IP address of the backend. This is a type alias for std::net::IpAddr.
pub type BackendAddress = IpAddr;

/// A type representing the different information about a Cueball backend.
#[derive(Clone, Debug)]
pub struct Backend {
    /// The concatenation of the backend address and port with a colon delimiter.
    pub name: BackendName,
    /// The address of the backend.
    pub address: BackendAddress,
    /// The port of the backend.
    pub port: BackendPort,
}

impl Backend {
    /// Return a new instance of `Backend` given a `BackendAddress` and `BackendPort`.
    pub fn new(address: &BackendAddress, port: BackendPort) -> Self {
        Backend {
            name: backend_name(address, port),
            address: *address,
            port,
        }
    }
}

// Concatentate the backend address and port with a colon delimiter.
fn backend_name(address: &BackendAddress, port: BackendPort) -> BackendName {
    let address_str = format!("{}", address);
    [address_str, String::from(":"), port.to_string()].concat()
}

/// Return a base64 encoded identifier based on the fields of the backend.
pub fn srv_key(backend: &Backend) -> BackendKey {
    let mut sha1 = Sha1::new();
    sha1.update(backend.name.as_bytes());
    sha1.update(b"||");
    sha1.update(backend.port.to_string().as_bytes());
    sha1.update(b"||");
    sha1.update(backend.address.to_string().as_bytes());

    base64::encode(&sha1.digest().bytes()).into()
}
