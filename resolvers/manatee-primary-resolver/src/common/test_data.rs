//
// Copyright 2020 Joyent, Inc
//

use std::str::FromStr;

use cueball::backend::{self, Backend, BackendAddress, BackendKey};
use cueball::resolver::{BackendAddedMsg, BackendRemovedMsg};

//
// Return a vector of some mock manatee ZooKeeper data, given the passed-in ip
// and port
//
pub fn json_vec(ip: &str, port: u16) -> Vec<u8> {
    //
    // Most of the data here isn't relevant, but real json from zookeeper
    // will include it, so we include it here.
    //
    format!(
        r#" {{
        "generation": 1,
        "primary": {{
            "id": "{ip}:{port}:12345",
            "ip": "{ip}",
            "pgUrl": "tcp://postgres@{ip}:{port}/postgres",
            "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
            "backupUrl": "http://{ip}:12345"
        }},
        "sync": {{
            "id": "10.77.77.21:5432:12345",
            "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
            "ip": "10.77.77.21",
            "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
            "backupUrl": "http://10.77.77.21:12345"
        }},
        "async": [],
        "deposed": [
            {{
                "id":"10.77.77.22:5432:12345",
                "ip": "10.77.77.22",
                "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                "backupUrl": "http://10.77.77.22:12345"
            }}
        ],
        "initWal": "0/16522D8"
    }}"#,
        ip = ip,
        port = port
    )
    .as_bytes()
    .to_vec()
}

///
/// Represents a valid backend
///
#[derive(Clone)]
pub struct BackendData {
    vec: Vec<u8>,
    object: Backend,
}

impl BackendData {
    pub fn new(ip: &str, port: u16) -> Self {
        BackendData {
            vec: json_vec(ip, port),
            object: Backend::new(
                &BackendAddress::from_str(ip).expect("Invalid IP address"),
                port,
            ),
        }
    }

    //
    // The below functions provide convenient ways to convert the data to
    // various related types.
    //

    pub fn raw_vec(&self) -> Vec<u8> {
        self.vec.clone()
    }

    pub fn key(&self) -> BackendKey {
        backend::srv_key(&self.object)
    }

    pub fn added_msg(&self) -> BackendAddedMsg {
        BackendAddedMsg {
            key: self.key(),
            backend: self.object.clone(),
        }
    }

    pub fn removed_msg(&self) -> BackendRemovedMsg {
        BackendRemovedMsg(self.key())
    }
}

//
// The rest of the functions here provide mock data for use in tests.
//

pub fn backend_ip1_port1() -> BackendData {
    BackendData::new("10.77.77.28", 5432)
}

pub fn backend_ip1_port2() -> BackendData {
    BackendData::new("10.77.77.28", 5431)
}

pub fn backend_ip2_port1() -> BackendData {
    BackendData::new("10.77.77.21", 5432)
}

pub fn backend_ip2_port2() -> BackendData {
    BackendData::new("10.77.77.21", 5431)
}

pub fn invalid_json_vec() -> Vec<u8> {
    b"foo".to_vec()
}

pub fn no_ip_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "sync": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "pgUrl": "tcp://postgres@10.77.77.28:5432/postgres",
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}

pub fn invalid_ip_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "ip": "foo",
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "sync": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "pgUrl": "tcp://postgres@10.77.77.28:5432/postgres",
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}

pub fn wrong_type_ip_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "ip": true,
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "sync": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "pgUrl": "tcp://postgres@10.77.77.28:5432/postgres",
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}

pub fn no_pg_url_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "sync": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "ip": "10.77.77.21",
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}

pub fn invalid_pg_url_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "pgUrl": "foo",
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "sync": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "ip": "10.77.77.21",
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}

pub fn wrong_type_pg_url_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "pgUrl": true,
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "sync": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "ip": "10.77.77.21",
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}

pub fn no_port_pg_url_vec() -> Vec<u8> {
    br#" {
            "generation": 1,
            "primary": {
                "id": "10.77.77.28:5432:12345",
                "ip": "10.77.77.28",
                "pgUrl": "tcp://postgres@10.77.77.22/postgres",
                "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                "backupUrl": "http://10.77.77.28:12345"
            },
            "sync": {
                "id": "10.77.77.21:5432:12345",
                "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                "ip": "10.77.77.21",
                "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                "backupUrl": "http://10.77.77.21:12345"
            },
            "async": [],
            "deposed": [
                {
                    "id":"10.77.77.22:5432:12345",
                    "ip": "10.77.77.22",
                    "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                    "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                    "backupUrl": "http://10.77.77.22:12345"
                }
            ],
            "initWal": "0/16522D8"
        }
    "#
    .to_vec()
}
