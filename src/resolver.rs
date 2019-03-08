/*
 * Copyright 2019 Joyent, Inc.
 */

use std::sync::mpsc::Sender;

use crate::backend;

pub trait Resolver: Send + 'static {
    fn start (&mut self, s: Sender<BackendMsg>);
    fn stop(&mut self);
    fn get_last_error(&self) -> Option<String>;
}

pub struct BackendAddedMsg {
    pub key: backend::BackendKey,
    pub backend: backend::Backend
}

pub struct BackendRemovedMsg(pub backend::BackendKey);

pub enum BackendMsg {
    AddedMsg(BackendAddedMsg),
    RemovedMsg(BackendRemovedMsg)
}
