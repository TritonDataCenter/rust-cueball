// Copyright 2019 Joyent, Inc.

use std::io::Error as IOError;
use std::net::{SocketAddr, TcpStream};
use std::ops::{Deref, DerefMut};

use cueball::backend::Backend;
use cueball::connection::Connection;

#[derive(Debug)]
pub struct TcpStreamWrapper {
    pub stream: Option<TcpStream>,
    addr: SocketAddr,
    connected: bool,
}

impl TcpStreamWrapper {
    pub fn new(b: &Backend) -> Self {
        let addr = SocketAddr::from((b.address, b.port));

        TcpStreamWrapper {
            stream: None,
            addr,
            connected: false,
        }
    }
}

impl Connection for TcpStreamWrapper {
    type Error = IOError;

    fn connect(&mut self) -> Result<(), Self::Error> {
        let stream = TcpStream::connect(&self.addr)?;
        self.stream = Some(stream);
        self.connected = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        self.stream = None;
        self.connected = false;
        Ok(())
    }
}

impl Deref for TcpStreamWrapper {
    type Target = TcpStream;

    fn deref(&self) -> &TcpStream {
        &self.stream.as_ref().unwrap()
    }
}

impl DerefMut for TcpStreamWrapper {
    fn deref_mut(&mut self) -> &mut TcpStream {
        self.stream.as_mut().unwrap()
    }
}
