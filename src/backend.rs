/*
 * Copyright 2019 Joyent, Inc.
 */

use std::net::IpAddr;

use base64;
use sha1::Sha1;

// TODO: At least BackendKey needs to be a newtype
pub type BackendKey = String;
pub type BackendPort = u16;
pub type BackendName = String;
pub type BackendAddress = IpAddr;

#[derive(Clone, Debug)]
pub struct Backend {
    pub name: BackendName,
    pub address: BackendAddress,
    pub port: BackendPort
}

impl Backend {
    pub fn new(address: &BackendAddress, port: &BackendPort) -> Self {
        Backend {
            name: backend_name(address, port),
            address: address.clone(),
            port: port.clone()
        }
    }
}

fn backend_name(address: &BackendAddress, port: &BackendPort) -> BackendName {
    let address_str = format!("{}", address);
    [address_str, String::from(":"), port.to_string()].concat()
}

pub fn srv_key(backend: &Backend) -> BackendKey {
    let mut sha1 = Sha1::new();
    sha1.update(backend.name.as_bytes());
    sha1.update(b"||");
    sha1.update(backend.port.to_string().as_bytes());
    sha1.update(b"||");
    sha1.update(backend.address.to_string().as_bytes());

    base64::encode(&sha1.digest().bytes())
}
