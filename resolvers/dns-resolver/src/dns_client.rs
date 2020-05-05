// Copyright 2020 Joyent, Inc

use slog::{debug, info, Logger};
use std::fs;
use std::io;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use newtype_derive::NewtypeFrom;
use resolv_conf::Config;
use trust_dns_client::client::{Client, SyncClient};
use trust_dns_client::error::ClientError;
use trust_dns_client::op::ResponseCode;
use trust_dns_client::rr::{DNSClass, RData, RecordType};
use trust_dns_client::udp::UdpClientConnection;
use trust_dns_proto::error::ProtoError;
use trust_dns_proto::rr::domain::Name;

use crate::resolver::ResolverError;

static DEFAULT_RESOLV_CONF: &str = "/etc/resolv.conf";

#[derive(Debug)]
pub struct ARecord {
    pub name: String,
    pub ttl: u32,
    pub address: IpAddr,
}

#[derive(Debug, Default)]
pub struct ARecordResp {
    pub answers: Vec<ARecord>,
}

#[derive(Debug, Default)]
pub struct SrvRecord {
    pub priority: u16,
    pub weight: u16,
    pub ttl: u32,
    pub port: u16,
    pub target: String,
}

#[derive(Debug, Default)]
pub struct SrvRecordResp {
    pub answers: Vec<SrvRecord>,
    pub additional: Vec<ARecord>,
}

pub trait DnsClient {
    fn query_srv(
        &self,
        name: &str,
        log: &Logger,
    ) -> Result<SrvRecordResp, ResolverError>;
    fn query_a(
        &self,
        name: &str,
        log: &Logger,
    ) -> Result<ARecordResp, ResolverError>;
}

pub struct TrustDnsClient(SyncClient<UdpClientConnection>);

NewtypeFrom! { () pub struct TrustDnsClient(SyncClient<UdpClientConnection>); }

impl DnsClient for TrustDnsClient {
    fn query_srv(
        &self,
        name: &str,
        log: &Logger,
    ) -> Result<SrvRecordResp, ResolverError> {
        let n = Name::from_utf8(name)?;
        let resp = self.0.query(&n, DNSClass::IN, RecordType::SRV)?;
        let mut srv_resp = SrvRecordResp::default();
        debug!(log, "srv_response: {:?}", resp);
        if resp.response_code() == ResponseCode::NoError {
            for srv in resp.answers() {
                if let RData::SRV(ref s_rec) = *srv.rdata() {
                    srv_resp.answers.push(SrvRecord {
                        port: s_rec.port(),
                        priority: s_rec.priority(),
                        target: s_rec.target().to_string(),
                        ttl: srv.ttl(),
                        weight: s_rec.weight(),
                    });
                }
            }
        }
        Ok(srv_resp)
    }

    fn query_a(
        &self,
        name: &str,
        log: &Logger,
    ) -> Result<ARecordResp, ResolverError> {
        let n = Name::from_utf8(name)?;
        let resp = self.0.query(&n, DNSClass::IN, RecordType::A)?;
        debug!(log, "a resp: {:?}", resp);
        let mut a_resp = ARecordResp::default();
        if resp.response_code() == ResponseCode::NoError {
            for a in resp.answers() {
                if let RData::A(ref ip) = *a.rdata() {
                    a_resp.answers.push(ARecord {
                        name: a.name().to_string(),
                        ttl: a.ttl(),
                        address: ip.to_string().parse::<IpAddr>()?,
                    });
                }
            }
        }
        Ok(a_resp)
    }
}

impl From<ClientError> for ResolverError {
    fn from(e: ClientError) -> Self {
        ResolverError::DnsClientError { err: e.to_string() }
    }
}

impl From<ProtoError> for ResolverError {
    fn from(e: ProtoError) -> Self {
        ResolverError::DnsClientError { err: e.to_string() }
    }
}

pub fn init_dns_client(
    resolver: &str,
) -> Result<Arc<dyn DnsClient + Send + Sync>, ResolverError> {
    let server = resolver.to_string().parse()?;
    let conn = UdpClientConnection::new(server)?;
    Ok(Arc::new(TrustDnsClient::from(SyncClient::new(conn))))
}

pub fn configure_resolvers(
    buf: &str,
    resolvers: &mut Vec<Arc<dyn DnsClient + Send + Sync>>,
    log: &Logger,
) -> Result<(), ResolverError> {
    let nameservers = parse_ns_resolv_conf(&buf)?;
    for ns in nameservers.iter() {
        let res = format!("{}:{}", ns.to_string(), 53);
        let resolver = init_dns_client(&res)?;
        info!(log, "Found resolver: {}", res);
        resolvers.push(resolver);
    }
    Ok(())
}

pub fn parse_ns_resolv_conf(buf: &str) -> Result<Vec<IpAddr>, ResolverError> {
    let cfg = match Config::parse(&buf) {
        Ok(c) => c,
        Err(e) => {
            return Err(ResolverError::ResolvConfInvalid { err: e.to_string() })
        }
    };
    let mut nameservers = Vec::new();
    for ns in cfg.nameservers {
        let addr = IpAddr::from_str(&ns.to_string())?;
        nameservers.push(addr);
    }
    Ok(nameservers)
}

pub fn read_resolv_conf(path: Option<String>) -> Result<String, io::Error> {
    let resolv_conf_path =
        path.unwrap_or_else(|| DEFAULT_RESOLV_CONF.to_string());
    let buf = fs::read(resolv_conf_path)?;
    Ok(std::str::from_utf8(&buf).unwrap().to_string())
}

#[test]
fn test_ns_resolv_conf_good_config() {
    let config_str = "
options ndots:8 timeout:8 attempts:8

domain joyent.us
search joyent.us

nameserver 10.77.77.92
nameserver 10.77.77.75
nameserver 10.77.77.88

options rotate
options inet6 no-tld-query

sortlist 130.155.160.0/255.255.240.0 130.155.0.0";

    let resolvers = match parse_ns_resolv_conf(&config_str) {
        Ok(r) => r,
        Err(_e) => {
            assert!(false);
            Vec::new()
        }
    };
    assert_eq!(resolvers.len(), 3);
}

#[test]
fn test_ns_resolv_conf_no_resolvers() {
    let config_str = "
options ndots:8 timeout:8 attempts:8

domain joyent.us
search joyent.us

options rotate
options inet6 no-tld-query

sortlist 130.155.160.0/255.255.240.0 130.155.0.0";

    let resolvers = match parse_ns_resolv_conf(&config_str) {
        Ok(r) => r,
        Err(_e) => {
            assert!(false);
            Vec::new()
        }
    };
    assert_eq!(resolvers.len(), 0);
}

#[test]
fn test_ns_resolv_conf_mangled_config() {
    let config_str = "
options ndots:8 timeout:8 attempts:8

domain joyent.us
search joyent.us

cheese 1.2.3.4

options rotate
options inet6 no-tld-query

sortlist 130.155.160.0/255.255.240.0 130.155.0.0";

    match parse_ns_resolv_conf(&config_str) {
        Ok(_) => assert!(false),
        Err(e) => {
            assert_eq!(e.to_string(), "Resolver configuration parsing failure");
        }
    };
}

#[test]
fn test_ns_resolv_conf_empty_config() {
    let config_str = "";
    match parse_ns_resolv_conf(&config_str) {
        Ok(r) => {
            assert_eq!(r.len(), 0);
        }
        Err(_) => {
            assert!(false);
        }
    };
}
