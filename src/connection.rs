/*
 * Copyright 2019 Joyent, Inc.
 */

use crate::backend::Backend;
use crate::error::Error;

pub trait Connection: Send + Sync + Sized + 'static {
    fn new(b: &Backend) -> Self;
    fn connect(&mut self) -> Result<(), Error>;
    fn close(&mut self) -> Result<(), Error>;
    fn set_unwanted(&self) -> ();
}
