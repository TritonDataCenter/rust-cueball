/*
 * Copyright 2019 Joyent, Inc.
 */

use std::sync::mpsc::Sender;

pub trait Resolver: Send + Sync
{
    fn new (&self, s: Sender<String>) -> Self;
    fn start(&self) -> ();
    fn stop(&self) -> ();
    fn get_last_error(&self) -> Option<String>;
}
