// Copyright 2020 Joyent, Inc.

// DNS resolver, a library providing SRV DNS records for cueball.
use std::collections::HashMap;
use std::convert::From;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::vec::Vec;

use chrono::prelude::*;
use chrono::{DateTime, Utc};
use cueball::backend;
use cueball::resolver::{
    BackendAddedMsg, BackendMsg, BackendRemovedMsg, Resolver,
};
use slog::{debug, error, info, warn, Logger};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use thiserror::Error;
use tokio::prelude::*;

use crate::dns_client::configure_resolvers;
use crate::dns_client::read_resolv_conf;
use crate::dns_client::DnsClient;

// DNS resolver; pass in the SRV record, and optional
// resolver vector.
pub struct DnsResolver {
    domain: String,
    service: String,
    resolvers: Option<Vec<Arc<dyn DnsClient + Send + Sync>>>,
    log: Logger,
}

impl DnsResolver {
    pub fn new(
        domain: String,
        service: String,
        resolvers: Option<Vec<Arc<dyn DnsClient + Send + Sync>>>,
        log: Logger,
    ) -> Self {
        DnsResolver {
            domain,
            service,
            resolvers: Some(resolvers.unwrap_or_default()),
            log: log.clone(),
        }
    }
}

impl Resolver for DnsResolver {
    fn run(&mut self, s: Sender<BackendMsg>) {
        let srv_retry_params = RetryParams::default();
        let srv_rec = SrvRec::default();
        let resolver = ResolverFSM::start(ResolverContext {
            resolvers: self.resolvers.as_ref().unwrap().to_vec(),
            backends: HashMap::new(),
            srv: srv_rec,
            srvs: Vec::new(),
            srv_rem: Vec::new(),
            service: self.service.clone(),
            domain: self.domain.clone(),
            last_srv_ttl: None,
            next_service: None,
            srv_retry_params,
            pool_tx: s,
            log: self.log.clone(),
        });

        let _fsm = thread::spawn(move || {
            let _result = resolver.wait();
        });
    }
}

#[derive(Debug, Default)]
struct RetryParams {
    max: u8,
    count: u8,
    timeout: u32,
    min_delay: u32,
    delay: u32,
    max_delay: u32,
}

#[derive(Clone, Debug, Default)]
struct SrvRec {
    name: String,
    port: u16,
    addresses_v4: Vec<IpAddr>,
    expiry_v4: Option<NaiveDateTime>,
}

#[allow(dead_code)]
struct ResolverContext {
    resolvers: Vec<Arc<dyn DnsClient + Send + Sync>>,
    backends: HashMap<String, backend::Backend>,
    domain: String,
    service: String,
    srv: SrvRec,
    srvs: Vec<SrvRec>,
    srv_rem: Vec<SrvRec>,
    last_srv_ttl: Option<u32>,
    next_service: Option<NaiveDateTime>,
    srv_retry_params: RetryParams,
    pool_tx: Sender<BackendMsg>,
    log: Logger,
}

#[derive(Debug)]
struct ResolverResult {
    result: String,
}

#[derive(Error, Debug)]
pub enum ResolverError {
    #[error("Initialization failed")]
    InitFailed(#[from] std::io::Error),
    #[error("IP address parsing failure")]
    IpAddressParsingFailed(#[from] std::net::AddrParseError),
    #[error("DNS client initialization failure")]
    DnsClientError { err: String },
    #[error("Resolver configuration parsing failure")]
    ResolvConfInvalid { err: String },
    #[error("Conversion to DNS Name failure")]
    ConvertStringtoDnsNameError { err: String },
    #[error("SRV record query failure")]
    SrvRecordQueryFailure { err: String },
    #[error("A record query failure")]
    ARecordQueryFailure { err: String },
    #[error("unknown resolver error")]
    Unknown,
}

#[derive(StateMachineFuture)]
#[state_machine_future(context = "ResolverContext")]
enum ResolverFSM {
    #[state_machine_future(start, transitions(CheckNs, Error))]
    Init,
    #[state_machine_future(transitions(Srv, Error))]
    CheckNs,
    #[state_machine_future(transitions(SrvTry, Error))]
    Srv,
    #[state_machine_future(transitions(SrvErr, Aaaa, Error))]
    SrvTry,
    #[state_machine_future(transitions(Aaaa, Error))]
    SrvErr,
    #[state_machine_future(transitions(AaaaNext, Error))]
    Aaaa,
    #[state_machine_future(transitions(AaaaTry, Error))]
    AaaaNext,
    #[state_machine_future(transitions(A))]
    AaaaTry,
    #[state_machine_future(transitions(ANext, Error))]
    A,
    #[state_machine_future(transitions(ANext, AErr))]
    ATry,
    #[state_machine_future(transitions(ANext, ATry))]
    AErr,
    #[state_machine_future(transitions(ATry, Process))]
    ANext,
    #[state_machine_future(transitions(Sleep))]
    Process,
    #[state_machine_future(transitions(Srv))]
    Sleep,
    #[state_machine_future(ready)]
    Stop(ResolverResult),
    #[state_machine_future(error)]
    Error(ResolverError),
}

impl PollResolverFSM for ResolverFSM {
    //startup
    fn poll_init<'s, 'c>(
        _init: &'s mut RentToOwn<'s, Init>,
        _context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterInit, ResolverError> {
        transition!(CheckNs)
    }

    // bootstrap resolvers, use resolv.conf if none specified
    fn poll_check_ns<'s, 'c>(
        _check_ns: &'s mut RentToOwn<'s, CheckNs>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterCheckNs, ResolverError> {
        if context.resolvers.is_empty() {
            info!(context.log, "Configuring from /etc/resolv.conf");
            let log = &context.log.clone();
            let cfg = &read_resolv_conf(None)?;
            configure_resolvers(&cfg, &mut context.resolvers, log)?;
        }

        transition!(Srv)
    }

    fn poll_srv<'s, 'c>(
        _srv: &'s mut RentToOwn<'s, Srv>,
        _context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterSrv, ResolverError> {
        transition!(SrvTry)
    }

    // look up the SRV records and populate the context.srvs vector
    // with name+port objects, each of which will become a backend
    fn poll_srv_try<'s, 'c>(
        _srv_try: &'s mut RentToOwn<'s, SrvTry>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterSrvTry, ResolverError> {
        let lookup_name = format!("{}.{}", context.service, context.domain);

        resolve_srv_records(&lookup_name, context)?;
        transition!(Aaaa)
    }

    // if no SRV records, populate with a dummy backend
    fn poll_srv_err<'s, 'c>(
        _srv_err: &'s mut RentToOwn<'s, SrvErr>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterSrvErr, ResolverError> {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let b = backend::Backend::new(&ip, 255);
        context.backends.insert(b.name.to_string(), b);

        transition!(Aaaa)
    }

    // IPv6 handling not implemented
    fn poll_aaaa<'s, 'c>(
        _aaaa: &'s mut RentToOwn<'s, Aaaa>,
        _context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterAaaa, ResolverError> {
        transition!(AaaaNext)
    }

    // IPv6 handling not implemented
    fn poll_aaaa_next<'s, 'c>(
        _aaaa_next: &'s mut RentToOwn<'s, AaaaNext>,
        _context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterAaaaNext, ResolverError> {
        transition!(AaaaTry)
    }

    // IPv6 handling not implemented
    fn poll_aaaa_try<'s, 'c>(
        _aaaa_try: &'s mut RentToOwn<'s, AaaaTry>,
        _context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterAaaaTry, ResolverError> {
        transition!(A)
    }

    // a, a_next and a_try iterate over each entry and fill out
    // IP address and expiry information
    fn poll_a<'s, 'c>(
        _a: &'s mut RentToOwn<'s, A>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterA, ResolverError> {
        context.srv_rem = context.srvs.clone();
        debug!(context.log, "context.srv_rem: {:?}", context.srv_rem);
        transition!(ANext)
    }

    fn poll_a_next<'s, 'c>(
        _a: &'s mut RentToOwn<'s, ANext>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterANext, ResolverError> {
        match context.srv_rem.pop() {
            Some(srv) => {
                context.srv = srv;
                transition!(ATry)
            }
            None => transition!(Process),
        }
    }

    fn poll_a_try<'s, 'c>(
        _a: &'s mut RentToOwn<'s, ATry>,
        mut context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterATry, ResolverError> {
        match resolve_a_records(&mut context) {
            Ok(_) => transition!(ANext),
            Err(e) => {
                error!(
                    context.log,
                    "Error resolving A record: {}",
                    e.to_string()
                );
                transition!(AErr)
            }
        }
    }

    fn poll_a_err<'s, 'c>(
        _a: &'s mut RentToOwn<'s, AErr>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterAErr, ResolverError> {
        info!(context.log, "srv_err: backends: {:?}", context.backends);
        transition!(ATry)
    }

    // process emits events to cueball for each backend
    fn poll_process<'s, 'c>(
        _process: &'s mut RentToOwn<'s, Process>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterProcess, ResolverError> {
        let mut new_backends: HashMap<String, backend::Backend> =
            HashMap::new();

        for srv in context.srvs.iter() {
            debug!(context.log, "processing backend srv {:?}", srv);
            for ip in srv.addresses_v4.iter() {
                let b = backend::Backend::new(&ip, srv.port);
                new_backends.entry(b.name.to_string()).or_insert(b);
            }
        }

        debug!(context.log, "new_backends len: {}", new_backends.len());
        if new_backends.keys().len() == 0 {
            info!(
                context.log,
                "found no DNS records for {}.{}, next service: {:?}",
                context.service,
                context.domain,
                context.next_service
            );
            context.srvs.clear();
            transition!(Sleep)
        }

        let mut added: HashMap<String, backend::Backend> = HashMap::new();
        let mut removed: HashMap<String, backend::Backend> = HashMap::new();

        context.backends.iter().for_each(|(k, v)| {
            removed.insert(k.to_string(), v.clone());
        });

        new_backends.iter().for_each(|(k, v)| {
            added.insert(k.to_string(), v.clone());
        });

        send_added_backends(
            added,
            context.pool_tx.clone(),
            context.log.clone(),
        );
        send_removed_backends(
            removed,
            context.pool_tx.clone(),
            context.log.clone(),
        );

        context.srvs.clear();

        context.backends = new_backends;

        transition!(Sleep)
    }

    fn poll_sleep<'s, 'c>(
        _sleep: &'s mut RentToOwn<'s, Sleep>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterSleep, ResolverError> {
        let now = Utc::now().naive_utc();

        let min_delay =
            context.next_service.unwrap().signed_duration_since(now);

        debug!(
            context.log,
            "sleeping for remainder of ttl: {:?}", min_delay
        );
        thread::sleep(min_delay.to_std().unwrap());
        transition!(Srv)
    }
}

fn resolve_srv_records(
    name: &str,
    context: &mut ResolverContext,
) -> Result<(), ResolverError> {
    for resolver in context.resolvers.iter_mut() {
        let srv_resp = match resolver.query_srv(&name, &context.log) {
            Ok(r) => r,
            Err(e) => {
                error!(
                    context.log,
                    "failed to resolve srv: {}, trying next resolver",
                    e.to_string()
                );
                continue;
            }
        };
        debug!(context.log, "srv_response: {:?}", srv_resp);
        for srv in srv_resp.answers.iter() {
            let ttl = srv.ttl;
            let now: DateTime<Utc> = Utc::now();
            let next: i64 = now.timestamp() + i64::from(ttl);
            let next_service = NaiveDateTime::from_timestamp(next, 0);
            context.last_srv_ttl = Some(ttl);
            context.next_service = Some(next_service);
            let port = srv.port;
            debug!(context.log, "srv port: {}", port);
            let lookup_name = SrvRec {
                name: srv.target.to_string(),
                port,
                addresses_v4: Vec::new(),
                expiry_v4: Some(next_service),
            };
            context.srvs.push(lookup_name);
            debug!(context.log, "context srvs: {:?}", context.srvs);
        }
    }
    if context.srvs.is_empty() {
        let now: DateTime<Utc> = Utc::now();
        let next: i64 = now.timestamp() + i64::from(5);
        let next_service = NaiveDateTime::from_timestamp(next, 0);
        debug!(context.log, "setting next service to {:?}", next_service);
        context.next_service = Some(next_service);
        let lookup_name = SrvRec {
            name: name.to_string(),
            port: 80,
            addresses_v4: Vec::new(),
            expiry_v4: Some(next_service),
        };
        context.srvs.push(lookup_name);
    }
    Ok(())
}

fn resolve_a_records(
    context: &mut ResolverContext,
) -> Result<(), ResolverError> {
    debug!(context.log, "querying a record for {:?}", context.srv.name);
    let name = &context.srv.name;
    let port = &context.srv.port;
    for resolver in context.resolvers.iter_mut() {
        let a_resp = match resolver.query_a(&context.srv.name, &context.log) {
            Ok(r) => r,
            Err(e) => {
                error!(
                    context.log,
                    "error resolving a record: {}, trying next resolver",
                    e.to_string()
                );
                continue;
            }
        };
        debug!(context.log, "a resp: {:?}", a_resp);
        for a in a_resp.answers.iter() {
            let addr = a.address.to_string().parse::<IpAddr>()?;
            match context
                .srvs
                .iter()
                .position(|s| (s.name == *name && s.port == *port))
            {
                Some(idx) => {
                    context.srvs[idx].addresses_v4.push(addr);
                    return Ok(());
                }
                None => {
                    warn!(context.log, "no backend found for name: {}", name)
                }
            }
        }
    }
    Ok(())
}

fn send_added_backends(
    to_send: HashMap<String, backend::Backend>,
    s: Sender<BackendMsg>,
    log: Logger,
) {
    to_send.iter().for_each(|(k, b)| {
        let backend_key = backend::srv_key(b);
        let backend_msg = BackendMsg::AddedMsg(BackendAddedMsg {
            key: backend_key,
            backend: b.clone(),
        });
        match s.send(backend_msg) {
            Ok(_) => info!(log, "resolver sent added msg: {}", k),
            Err(e) => warn!(log, "could not msg: {}", e),
        }
    });
}

fn send_removed_backends(
    to_send: HashMap<String, backend::Backend>,
    s: Sender<BackendMsg>,
    log: Logger,
) {
    to_send.iter().for_each(|(k, b)| {
        let backend_key = backend::srv_key(b);
        let backend_msg =
            BackendMsg::RemovedMsg(BackendRemovedMsg(backend_key));
        match s.send(backend_msg) {
            Ok(_) => info!(log, "resolver sent removed msg: {}", k),
            Err(e) => warn!(log, "could not msg: {}", e),
        }
    });
}

#[cfg(test)]
mod tests {
    use crate::dns_client::{
        ARecord, ARecordResp, DnsClient, SrvRecord, SrvRecordResp,
    };
    use crate::resolver::{DnsResolver, ResolverError};
    use cueball::resolver::{BackendMsg, Resolver};
    use slog::{debug, o, Drain, Logger};
    use std::net::IpAddr;
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn test_resolver_single_srv() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let log = Logger::root(
            Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
            o!("build-id" => "0.0.1"),
        );

        struct MockDnsClient {}
        impl DnsClient for MockDnsClient {
            fn query_srv(
                &self,
                _name: &str,
                _log: &Logger,
            ) -> Result<SrvRecordResp, ResolverError> {
                let mut srv_resp = SrvRecordResp::default();
                srv_resp.answers.push(SrvRecord {
                    port: 123,
                    priority: 1,
                    target: "_tcp.cheesy_record.joyent.us".to_string(),
                    ttl: 60,
                    weight: 100,
                });
                Ok(srv_resp)
            }

            fn query_a(
                &self,
                _name: &str,
                _log: &Logger,
            ) -> Result<ARecordResp, ResolverError> {
                let mut a_resp = ARecordResp::default();
                a_resp.answers.push(ARecord {
                    name: "cheesy_record.joyent.us".to_string(),
                    ttl: 60,
                    address: "1.2.3.4".parse::<IpAddr>()?,
                });
                Ok(a_resp)
            }
        }

        let mock_resolvers = MockDnsClient {};
        let mut resolvers: Vec<Arc<dyn DnsClient + Send + Sync>> = Vec::new();
        resolvers.push(Arc::new(mock_resolvers));

        let mut dr = DnsResolver::new(
            "cheesy_record.joyent.us.".to_string(),
            "_tcp".to_string(),
            Some(resolvers),
            log.clone(),
        );

        let (sender, receiver) = channel();

        dr.run(sender);
        let backend_msg = match receiver.recv() {
            Ok(BackendMsg::AddedMsg(added_msg)) => Ok(added_msg),
            _ => Err(()),
        };
        match backend_msg {
            Ok(r) => {
                assert_eq!(r.backend.address.to_string(), "1.2.3.4");
                assert_eq!(r.backend.port, 123);
            }
            Err(_) => assert!(false),
        }
    }

    #[test]
    fn test_resolver_multi_srv() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let log = Logger::root(
            Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
            o!("build-id" => "0.0.1"),
        );
        struct MockDnsClient {}
        impl DnsClient for MockDnsClient {
            fn query_srv(
                &self,
                _name: &str,
                _log: &Logger,
            ) -> Result<SrvRecordResp, ResolverError> {
                let mut srv_resp = SrvRecordResp::default();
                srv_resp.answers.push(SrvRecord {
                    port: 1,
                    priority: 1,
                    target: "_tcp.cheesy_record.joyent.us".to_string(),
                    ttl: 60,
                    weight: 100,
                });
                srv_resp.answers.push(SrvRecord {
                    port: 2,
                    priority: 1,
                    target: "_tcp.cheesy_record.joyent.us".to_string(),
                    ttl: 60,
                    weight: 100,
                });
                Ok(srv_resp)
            }

            fn query_a(
                &self,
                _name: &str,
                _log: &Logger,
            ) -> Result<ARecordResp, ResolverError> {
                let mut a_resp = ARecordResp::default();
                a_resp.answers.push(ARecord {
                    name: "cheesy_record.joyent.us".to_string(),
                    ttl: 60,
                    address: "1.2.3.4".parse::<IpAddr>()?,
                });
                Ok(a_resp)
            }
        }

        let mock_resolvers = MockDnsClient {};
        let mut resolvers: Vec<Arc<dyn DnsClient + Send + Sync>> = Vec::new();
        resolvers.push(Arc::new(mock_resolvers));

        let mut dr = DnsResolver::new(
            "cheesy_record.joyent.us.".to_string(),
            "_tcp".to_string(),
            Some(resolvers),
            log.clone(),
        );

        let (sender, receiver) = channel();

        dr.run(sender);

        for _ in 0..1 {
            let backend_msg = match receiver.recv() {
                Ok(BackendMsg::AddedMsg(added_msg)) => Ok(added_msg),
                _ => Err(()),
            };
            match backend_msg {
                Ok(r) => {
                    assert_eq!(r.backend.address.to_string(), "1.2.3.4");
                    assert!(r.backend.port < 3);
                }
                Err(_) => assert!(false),
            }
        }
    }

    #[test]
    fn test_resolver_interval() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let log = Logger::root(
            Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
            o!("build-id" => "0.0.1"),
        );

        struct MockDnsClient {}
        impl DnsClient for MockDnsClient {
            fn query_srv(
                &self,
                _name: &str,
                _log: &Logger,
            ) -> Result<SrvRecordResp, ResolverError> {
                let mut srv_resp = SrvRecordResp::default();
                srv_resp.answers.push(SrvRecord {
                    port: 1,
                    priority: 1,
                    target: "_tcp.cheesy_record.joyent.us".to_string(),
                    ttl: 2,
                    weight: 100,
                });
                srv_resp.answers.push(SrvRecord {
                    port: 2,
                    priority: 1,
                    target: "_tcp.cheesy_record.joyent.us".to_string(),
                    ttl: 2,
                    weight: 100,
                });
                Ok(srv_resp)
            }

            fn query_a(
                &self,
                _name: &str,
                _log: &Logger,
            ) -> Result<ARecordResp, ResolverError> {
                let mut a_resp = ARecordResp::default();
                a_resp.answers.push(ARecord {
                    name: "cheesy_record.joyent.us".to_string(),
                    ttl: 60,
                    address: "1.2.3.4".parse::<IpAddr>()?,
                });
                Ok(a_resp)
            }
        }

        let mock_resolvers = MockDnsClient {};
        let mut resolvers: Vec<Arc<dyn DnsClient + Send + Sync>> = Vec::new();
        resolvers.push(Arc::new(mock_resolvers));

        let mut dr = DnsResolver::new(
            "cheesy_record.joyent.us.".to_string(),
            "_tcp".to_string(),
            Some(resolvers),
            log.clone(),
        );

        let (sender, receiver) = channel();

        dr.run(sender);

        for _ in 0..6 {
            match receiver.recv() {
                Ok(BackendMsg::AddedMsg(added_msg)) => {
                    debug!(log, "Recved add backend {}", added_msg.key);
                    assert!(true);
                }
                Ok(BackendMsg::RemovedMsg(removed_msg)) => {
                    debug!(log, "Recvd rem backend {:?}", removed_msg.0);
                    assert!(true);
                }
                Ok(BackendMsg::StopMsg) => {
                    debug!(log, "Recvd stop resolver");
                    assert!(true);
                }
                _ => {
                    debug!(log, "received error");
                    assert!(false);
                }
            };
        }
    }
}
