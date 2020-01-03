// Copyright 2019 Joyent, Inc.

use std::sync::mpsc::Sender;

use cueball::backend::*;
use cueball::resolver::{BackendAddedMsg, BackendMsg, Resolver};

pub struct StaticIpResolver {
    backends: Vec<(BackendAddress, BackendPort)>,
    pool_tx: Option<Sender<BackendMsg>>,
    started: bool,
}

impl StaticIpResolver {
    pub fn new(backends: Vec<(BackendAddress, BackendPort)>) -> Self {
        StaticIpResolver {
            backends: backends,
            pool_tx: None,
            started: false,
        }
    }
}

impl Resolver for StaticIpResolver {
    fn run(&mut self, s: Sender<BackendMsg>) {
        if !self.started {
            self.backends.iter().for_each(|b| {
                let backend = Backend::new(&b.0, b.1);
                let backend_key = srv_key(&backend);
                let backend_msg = BackendMsg::AddedMsg(BackendAddedMsg {
                    key: backend_key,
                    backend: backend,
                });
                s.send(backend_msg).unwrap();
            });
            self.pool_tx = Some(s);
            self.started = true;
        }
    }
}
