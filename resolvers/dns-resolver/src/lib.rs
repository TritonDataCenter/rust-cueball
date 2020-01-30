// Copyright 2020 Joyent, Inc.

//! resolver is a library for resolving DNS records for cueball.
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::mpsc::Sender;
use std::thread;
use std::vec::Vec;

use chrono::prelude::*;
use chrono::{DateTime, Utc};
use cueball::backend;
use cueball::resolver::{BackendAddedMsg, BackendMsg, Resolver};
use slog::{debug, error, info, warn, Logger};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use tokio::prelude::*;
use trust_dns::client::{Client, SyncClient};
use trust_dns::op::ResponseCode;
use trust_dns::rr::{DNSClass, Name, RData, RecordType};
use trust_dns::udp::UdpClientConnection;

// TODO:
// - checking based on A record TTLs
// - retries
// - tests
// - shutdown
// - last error
// - comments
// - unused (but used) import errors

#[derive(Debug)]
pub struct BackendRemovedMsg(pub backend::BackendKey);

pub enum BackendAction {
    BackendAdded,
    BackendRemoved,
}

#[derive(Debug)]
pub struct DnsResolver {
    domain: String,
    service: String,
    log: Logger,
}

impl DnsResolver {
    pub fn new(domain: String, service: String, log: Logger) -> Self {
        DnsResolver {
            domain,
            service,
            log: log.clone(),
        }
    }
}

impl Resolver for DnsResolver {
    fn run(&mut self, s: Sender<BackendMsg>) {
        let srv_retry_params = RetryParams {
            max: 0,
            count: 0,
            timeout: 1000,
            min_delay: 0,
            delay: 1000,
            max_delay: 10000,
        };

        let srv_rec = SrvRec {
            name: String::new(),
            port: 0,
            addresses_v4: Vec::new(),
            expiry_v4: None,
        };

        let resolver = ResolverFSM::start(ResolverContext {
            resolvers: Vec::new(),
            backends: HashMap::new(),
            srv: srv_rec,
            srvs: Vec::new(),
            srv_rem: Vec::new(),
            service: self.service.clone(),
            domain: self.domain.clone(),
            last_srv_ttl: None,
            next_service: None,
            srv_retry_params,
            dns_client: None,
            defport: Some(80),
            pool_tx: s,
            log: self.log.clone(),
        });

        let _fsm = thread::spawn(move || {
            let _result = resolver.wait();
        });
    }
}

#[derive(Debug)]
struct RetryParams {
    max: u8,
    count: u8,
    timeout: u32,
    min_delay: u32,
    delay: u32,
    max_delay: u32,
}

#[derive(Clone, Debug)]
struct SrvRec {
    name: String,
    port: u16,
    addresses_v4: Vec<IpAddr>,
    expiry_v4: Option<NaiveDateTime>,
}

struct ResolverContext {
    resolvers: Vec<String>,
    backends: HashMap<String, backend::Backend>,
    domain: String,
    service: String,
    srv: SrvRec,
    srvs: Vec<SrvRec>,
    srv_rem: Vec<SrvRec>,
    last_srv_ttl: Option<u32>,
    next_service: Option<NaiveDateTime>,
    srv_retry_params: RetryParams,
    dns_client: Option<SyncClient<UdpClientConnection>>,
    pool_tx: Sender<BackendMsg>,
    defport: Option<u16>,
    log: Logger,
}

#[derive(Debug)]
struct ResolverResult {
    result: String,
}

#[derive(Debug)]
struct ResolverError {
    err: String,
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
        if !configure_from_resolv_conf(context) {
            configure_default_resolvers(context);
        }

        info!(context.log, "resolvers specified: {:?}", context.resolvers);

        context.dns_client = init_dns_client(&context.resolvers[0]);

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
        let name = Name::from_str(&lookup_name).unwrap();
        let dns_client = context.dns_client.as_ref();
        let domain = &context.domain;

        match dns_client {
            Some(client) => {
                match client.query(&name, DNSClass::IN, RecordType::SRV) {
                    Ok(srv_resp) => {
                        debug!(context.log, "srv_response: {:?}", srv_resp);
                        if srv_resp.response_code() == ResponseCode::NoError {
                            for srv in srv_resp.answers() {
                                if let RData::SRV(ref s_rec) = *srv.rdata() {
                                    let ttl = srv.ttl();
                                    let now: DateTime<Utc> = Utc::now();
                                    let next: i64 =
                                        now.timestamp() + i64::from(1000 * ttl);
                                    let next_service =
                                        NaiveDateTime::from_timestamp(next, 0);
                                    context.last_srv_ttl = Some(ttl);
                                    context.next_service = Some(next_service);
                                    let port = s_rec
                                        .port()
                                        .to_string()
                                        .parse::<u16>()
                                        .unwrap();
                                    let lookup_name = SrvRec {
                                        name: s_rec.target().to_string(),
                                        port,
                                        addresses_v4: Vec::new(),
                                        expiry_v4: Some(next_service),
                                    };
                                    context.srvs.push(lookup_name);
                                    debug!(
                                        context.log,
                                        "context srvs: {:?}", context.srvs
                                    );
                                }
                            }
                        } else if srv_resp.response_code()
                            == ResponseCode::NotImp
                            || srv_resp.response_code()
                                == ResponseCode::NXDomain
                        {
                            let now: DateTime<Utc> = Utc::now();
                            let next: i64 =
                                now.timestamp() + i64::from(1000 * 60 * 60);
                            let next_service =
                                NaiveDateTime::from_timestamp(next, 0);

                            let lookup_name = SrvRec {
                                name: domain.to_string(),
                                port: context.defport.unwrap(),
                                addresses_v4: Vec::new(),
                                expiry_v4: Some(next_service),
                            };

                            context.srvs.push(lookup_name);
                        } else if srv_resp.response_code()
                            == ResponseCode::Refused
                        {
                            context.srv_retry_params.count = 0;
                            transition!(SrvErr)
                        }
                    }
                    Err(e) => panic!("Got error on query {}", e),
                }
            }
            None => panic!("No client configured!"),
        };

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
        match context.srvs.iter().position(|s| s.name == context.srv.name) {
            Some(idx) => {
                context.srvs[idx].addresses_v4 =
                    context.srv.addresses_v4.clone()
            }
            None => debug!(
                context.log,
                "scan of srv records for {} complete.", context.srv.name
            ),
        };

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
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterATry, ResolverError> {
        let client = context.dns_client.as_ref();
        let srv = &context.srv;

        match client {
            Some(client) => {
                debug!(context.log, "querying a record for {:?}", srv);
                let target = Name::from_str(&srv.name).unwrap();
                match client.query(&target, DNSClass::IN, RecordType::A) {
                    Ok(a_resp) => {
                        debug!(context.log, "a resp: {:?}", a_resp);
                        if a_resp.response_code() == ResponseCode::NoError {
                            for a in a_resp.answers() {
                                if let RData::A(ref ip) = *a.rdata() {
                                    let _ttl = a.ttl();
                                    match ip.to_string().parse::<IpAddr>() {
                                        Ok(addr) => {
                                            context.srv.addresses_v4.push(addr)
                                        }
                                        Err(e) => error!(
                                            context.log,
                                            "could not parse ip address: {}", e
                                        ),
                                    }
                                }
                            }
                        } else {
                            transition!(AErr)
                        }
                    }
                    Err(_) => {
                        info!(context.log, "a resp error");
                        transition!(AErr)
                    }
                };
            }
            None => {
                error!(context.log, "ERROR no dns client!");
                transition!(AErr)
            }
        };

        transition!(ANext)
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
            for ip in srv.addresses_v4.iter() {
                let b = backend::Backend::new(&ip, srv.port);
                new_backends.entry(b.name.to_string()).or_insert(b);
            }
        }

        if new_backends.keys().len() == 0 {
            info!(
                context.log,
                "found no DNS records for {}.{}",
                context.service,
                context.domain
            );
            transition!(Sleep)
        }

        let mut added: HashMap<String, backend::Backend> = HashMap::new();
        let mut removed: HashMap<String, backend::Backend> = HashMap::new();

        context.backends.iter().for_each(|(k, v)| {
            if !new_backends.contains_key(k) {
                removed.insert(k.to_string(), v.clone());
            }
        });

        new_backends.iter().for_each(|(k, v)| {
            if !context.backends.contains_key(k) {
                added.insert(k.to_string(), v.clone());
            }
        });

        send_updates(added, context.pool_tx.clone(), context.log.clone());
        send_updates(removed, context.pool_tx.clone(), context.log.clone());

        context.srvs.clear();

        context.backends = new_backends.clone();

        transition!(Sleep)
    }

    fn poll_sleep<'s, 'c>(
        _sleep: &'s mut RentToOwn<'s, Sleep>,
        context: &'c mut RentToOwn<'c, ResolverContext>,
    ) -> Poll<AfterSleep, ResolverError> {
        let now = Utc::now().naive_utc();

        let min_delay =
            context.next_service.unwrap().signed_duration_since(now);

        info!(context.log, "sleeping for {}", min_delay);
        thread::sleep(min_delay.to_std().unwrap());
        transition!(Srv)
    }
}

fn init_dns_client(resolver: &str) -> Option<SyncClient<UdpClientConnection>> {
    match resolver.to_string().parse() {
        Ok(server) => match UdpClientConnection::new(server) {
            Ok(conn) => Some(SyncClient::new(conn)),
            Err(e) => {
                panic!("couldn't start a new DNS client connection: {}", e)
            }
        },
        Err(e) => panic!("could not parse resolver ip: {}", e),
    }
}

fn configure_default_resolvers(context: &mut ResolverContext) {
    context.resolvers.push("8.8.8.8:53".to_string());
    context.resolvers.push("8.8.8.4:53".to_string());
}

fn configure_from_resolv_conf(context: &mut ResolverContext) -> bool {
    let mut buf = Vec::with_capacity(4096);
    match File::open("/etc/resolv.conf") {
        Ok(mut f) => match f.read_to_end(&mut buf) {
            Ok(_) => {
                let cfg = resolv_conf::Config::parse(&buf).unwrap();
                for ns in cfg.nameservers.iter() {
                    let res = format!("{}:{}", ns.to_string(), 53);
                    context.resolvers.push(res);
                }
                true
            }
            Err(e) => {
                println!("Parse /etc/resolv.conf: {}", e);
                false
            }
        },
        Err(e) => {
            println!("Could not open resolve.conf: {}", e);
            false
        }
    }
}

fn send_updates(
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
            Ok(_) => info!(log, "resolver sent msg: {}", k),
            Err(e) => warn!(log, "could not msg: {}", e),
        }
    });
}
