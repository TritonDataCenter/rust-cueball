/*
 * Copyright 2019 Joyent, Inc.
 */

use crate::error::Error;

pub trait Connection: Send + Sync {
    type Connection: Send;

    fn connect(&self) -> Result<Self::Connection, Error>;
    fn close(&self) -> Result<(), Error>;
    fn set_unwanted(&self) -> ();
}
