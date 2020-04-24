// Copyright 2020 Joyent, Inc

use slog::{debug, Logger};
use std::fs::File;
use std::io::Read;
use std::net::IpAddr;
use std::sync::Arc;

use trust_dns_client::client::{Client, SyncClient};
use trust_dns_client::error::ClientError;
use trust_dns_client::op::ResponseCode;
use trust_dns_client::rr::{DNSClass, RData, RecordType};
use trust_dns_client::udp::UdpClientConnection;
use trust_dns_proto::error::ProtoError;
use trust_dns_proto::rr::domain::Name;

use crate::resolver::ResolverError;

static DEFAULT_RESOLV_CONF: &str = "/etc/resolv.conf";
const DEFAULT_DNS_PORT: u16 = 53;
const DEFAULT_RESOLVERS: &[&str] = &["8.8.8.8", "8.8.4.4"];

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
    match UdpClientConnection::new(server) {
        Ok(conn) => Ok(Arc::new(TrustDnsClient::from(SyncClient::new(conn)))),
        Err(e) => Err(ResolverError::DnsClientError { err: e.to_string() }),
    }
}

pub fn configure_default_resolvers(
    resolvers: &mut Vec<Arc<dyn DnsClient + Send + Sync>>,
) -> Result<(), ResolverError> {
    for ns in DEFAULT_RESOLVERS.iter() {
        let res = format!("{}:{}", (*ns).to_string(), DEFAULT_DNS_PORT);
        let resolver = init_dns_client(&res)?;
        resolvers.push(resolver);
    }
    Ok(())
}

pub fn configure_from_resolv_conf(
    resolvers: &mut Vec<Arc<dyn DnsClient + Send + Sync>>,
) -> Result<(), ResolverError> {
    let mut buf = Vec::new();
    let mut f = File::open(DEFAULT_RESOLV_CONF)?;
    f.read_exact(&mut buf)?;
    let cfg = match resolv_conf::Config::parse(&buf) {
        Ok(c) => c,
        Err(e) => {
            return Err(ResolverError::ResolvConfInvalid { err: e.to_string() })
        }
    };
    for ns in cfg.nameservers.iter() {
        let res = format!("{}:{}", ns.to_string(), 53);
        let resolver = init_dns_client(&res)?;
        resolvers.push(resolver);
    }
    Ok(())
}
