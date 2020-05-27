/*
 * Copyright 2020 Joyent, Inc.
 */

use std::ops::{Deref, DerefMut};

use native_tls::Certificate as NativeCertificate;
use native_tls::Error as NativeError;
use native_tls::TlsConnector;
use postgres;
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde_derive::Deserialize;

use cueball::backend::Backend;
use cueball::connection::Connection;

pub struct PostgresConnection {
    pub connection: Option<Client>,
    url: String,
    tls_config: TlsConfig,
    connected: bool,
}

impl PostgresConnection {
    pub fn connection_creator<'a>(
        mut config: PostgresConnectionConfig,
    ) -> impl FnMut(&Backend) -> PostgresConnection + 'a {
        move |b| {
            config.host = Some(b.address.to_string());
            config.port = Some(b.port);

            let url = config.to_owned().into();

            PostgresConnection {
                connection: None,
                url,
                tls_config: config.tls_config.clone(),
                connected: false,
            }
        }
    }
}

impl Connection for PostgresConnection {
    type Error = postgres::Error;

    fn connect(&mut self) -> Result<(), Self::Error> {
        let connection =
            if let Some(tls_connector) = make_tls_connector(&self.tls_config) {
                Client::connect(&self.url, tls_connector)?
            } else {
                Client::connect(&self.url, NoTls)?
            };
        self.connection = Some(connection);
        self.connected = true;
        Ok(())
    }

    fn is_valid(&mut self) -> bool {
        self.connection
            .as_mut()
            .unwrap()
            .simple_query("")
            .map(|_| ())
            .is_ok()
    }

    fn has_broken(&self) -> bool {
        match &self.connection {
            Some(conn) => conn.is_closed(),
            None => false,
        }
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        self.connection = None;
        self.connected = false;
        Ok(())
    }
}

impl Deref for PostgresConnection {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.connection.as_ref().unwrap()
    }
}

impl DerefMut for PostgresConnection {
    fn deref_mut(&mut self) -> &mut Client {
        self.connection.as_mut().unwrap()
    }
}

#[derive(Clone)]
pub struct PostgresConnectionConfig {
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub application_name: Option<String>,
    pub tls_config: TlsConfig,
}

impl From<PostgresConnectionConfig> for String {
    fn from(config: PostgresConnectionConfig) -> Self {
        let scheme = "postgresql://";
        let user = config.user.unwrap_or_else(|| "".into());

        let at = if user.is_empty() { "" } else { "@" };

        let host = config.host.unwrap_or_else(|| String::from("localhost"));
        let port = config
            .port
            .map(|p| p.to_string())
            .unwrap_or_else(|| "".to_string());

        let colon = if port.is_empty() { "" } else { ":" };

        let database = config.database.unwrap_or_else(|| "".into());

        let slash = if database.is_empty() { "" } else { "/" };

        let application_name =
            config.application_name.unwrap_or_else(|| "".into());
        let question_mark = "?";

        let app_name_param = if application_name.is_empty() {
            ""
        } else {
            "application_name="
        };

        let ssl_mode = config.tls_config.mode.to_string();
        let ssl_mode_param = if application_name.is_empty() {
            "sslmode="
        } else {
            "&sslmode="
        };

        [
            scheme,
            user.as_str(),
            at,
            host.as_str(),
            colon,
            port.as_str(),
            slash,
            database.as_str(),
            question_mark,
            app_name_param,
            application_name.as_str(),
            ssl_mode_param,
            ssl_mode.as_str(),
        ]
        .concat()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum TlsConnectMode {
    #[serde(alias = "disable")]
    Disable,
    #[serde(alias = "allow")]
    Allow,
    #[serde(alias = "prefer")]
    Prefer,
    #[serde(alias = "require")]
    Require,
    #[serde(alias = "verify-ca")]
    VerifyCa,
    #[serde(alias = "verify-full")]
    VerifyFull,
}

impl ToString for TlsConnectMode {
    fn to_string(&self) -> String {
        match self {
            TlsConnectMode::Disable => String::from("disable"),
            TlsConnectMode::Allow => String::from("allow"),
            TlsConnectMode::Prefer => String::from("prefer"),
            TlsConnectMode::Require => String::from("require"),
            TlsConnectMode::VerifyCa => String::from("verify-ca"),
            TlsConnectMode::VerifyFull => String::from("verify-full"),
        }
    }
}

/// An X509 certificate.
pub type Certificate = NativeCertificate;

/// An error returned from the TLS implementation.
pub type CertificateError = NativeError;

#[derive(Clone)]
pub struct TlsConfig {
    pub(self) mode: TlsConnectMode,
    pub(self) certificate: Option<Certificate>,
}

impl TlsConfig {
    pub fn disable() -> Self {
        TlsConfig {
            mode: TlsConnectMode::Disable,
            certificate: None,
        }
    }

    pub fn allow(certificate: Option<Certificate>) -> Self {
        TlsConfig {
            mode: TlsConnectMode::Allow,
            certificate,
        }
    }

    pub fn prefer(certificate: Option<Certificate>) -> Self {
        TlsConfig {
            mode: TlsConnectMode::Prefer,
            certificate,
        }
    }

    pub fn require(certificate: Option<Certificate>) -> Self {
        TlsConfig {
            mode: TlsConnectMode::Require,
            certificate,
        }
    }

    pub fn verify_ca(certificate: Certificate) -> Self {
        TlsConfig {
            mode: TlsConnectMode::VerifyCa,
            certificate: Some(certificate),
        }
    }

    pub fn verify_full(certificate: Certificate) -> Self {
        TlsConfig {
            mode: TlsConnectMode::VerifyFull,
            certificate: Some(certificate),
        }
    }
}

fn make_tls_connector(tls_config: &TlsConfig) -> Option<MakeTlsConnector> {
    let m_cert = tls_config.certificate.clone();
    match tls_config.mode {
        TlsConnectMode::Disable => None,
        TlsConnectMode::Allow
        | TlsConnectMode::Prefer
        | TlsConnectMode::Require => {
            if let Some(cert) = m_cert {
                // root cert supplied, use it to verify server certs
                let connector = TlsConnector::builder()
                    .add_root_certificate(cert)
                    .build()
                    .unwrap();
                let connector = MakeTlsConnector::new(connector);
                Some(connector)
            } else {
                // no cert is given, disable certificate verification
                // should we emit a warning to stderr since the function has "danger" in it?
                let connector = TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .build()
                    .unwrap();
                let connector = MakeTlsConnector::new(connector);
                Some(connector)
            }
        }
        TlsConnectMode::VerifyCa | TlsConnectMode::VerifyFull => {
            let cert = m_cert.expect(
                "A certificate is required for \
                 verify-ca, and verify-full SSL modes",
            );
            let connector = TlsConnector::builder()
                .add_root_certificate(cert)
                .build()
                .unwrap();
            Some(MakeTlsConnector::new(connector))
        }
    }
}
