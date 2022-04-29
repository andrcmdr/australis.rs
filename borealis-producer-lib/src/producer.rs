use borealis_types::types::BorealisMessage;
use nats;
use serde::Serialize;
use tracing::info;
use tracing_subscriber::EnvFilter;

use core::str::FromStr;

pub type Error = Box<dyn std::error::Error + 'static>;

/// Initialize logging
pub fn init_logging() {

    // Filters can be customized through RUST_LOG environment variable via CLI
    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,near=error,stats=info,telemetry=info,near-performance-metrics=info,aggregated=info,near_indexer=info,borealis_indexer=info,borealis_consumer=info",
    );

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stdout)
        .init();

}

/// Options to run NATS Producer
#[derive(Debug, Clone)]
pub struct Context {
    /// root CA certificate
    pub root_cert_path: Option<std::path::PathBuf>,
    /// client certificate
    pub client_cert_path: Option<std::path::PathBuf>,
    /// client private key
    pub client_private_key: Option<std::path::PathBuf>,
    /// Path to NATS credentials (JWT/NKEY tokens)
    pub creds_path: Option<std::path::PathBuf>,
    /// Borealis Bus (NATS based MOM/MQ/SOA service bus) protocol://address:port
    /// Example: "nats://borealis.aurora.dev:4222" or "tls://borealis.aurora.dev:4443" for TLS connection
    // default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    pub nats_server: String,
    /// Stream messages to subject
    // default_value = "BlockIndex_StreamerMessages"
    pub subject: String,
    /// Streaming messages format (`CBOR` or `JSON`), suffix for subject name
    // default_value = "CBOR"
    pub msg_format: MsgFormat,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            root_cert_path: Some(std::path::PathBuf::from("./.nats/seed/root-ca.crt")),
            client_cert_path: None,
            client_private_key: None,
            creds_path: Some(std::path::PathBuf::from("./.nats/seed/nats.creds")),
            nats_server: "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222".to_string(),
            subject: "BlockIndex_StreamerMessages_mainnet".to_string(),
            msg_format: MsgFormat::Cbor,
        }
    }
}

/// Streaming messages format (should be upper case, 'cause it's a suffix for `subject` name, and NATS subject is case sensitive)
#[derive(Debug, Clone, Copy)]
pub enum MsgFormat {
    Cbor,
    Json,
}

impl FromStr for MsgFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "cbor" => Ok(MsgFormat::Cbor),
            "json" => Ok(MsgFormat::Json),
            _ => Err(
                "Unknown message format: `--msg-fomat` should contain `CBOR` or `JSON`"
                    .to_string()
                    .into(),
            ),
        }
    }
}

impl ToString for MsgFormat {
    fn to_string(&self) -> String {
        match self {
            MsgFormat::Cbor => String::from("CBOR"),
            MsgFormat::Json => String::from("JSON"),
        }
    }
}

/// Data provider methods from Context for Producer trait
impl Producer for Context {
    /// root CA certificate
    fn root_cert_path(&self) -> Option<std::path::PathBuf> {
        return self.to_owned().root_cert_path;
    }

    /// client certificate
    fn client_cert_path(&self) -> Option<std::path::PathBuf> {
        return self.to_owned().client_cert_path;
    }

    /// client private key
    fn client_private_key(&self) -> Option<std::path::PathBuf> {
        return self.to_owned().client_private_key;
    }

    /// Path to NATS credentials (JWT/NKEY tokens)
    fn creds_path(&self) -> Option<std::path::PathBuf> {
        return self.to_owned().creds_path;
    }

    /// Borealis Bus (NATS based MOM/MQ/SOA service bus) protocol://address:port
    /// Example: "nats://borealis.aurora.dev:4222" or "tls://borealis.aurora.dev:4443" for TLS connection
    // default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    fn nats_server(&self) -> String {
        return self.to_owned().nats_server;
    }

    /// Stream messages to subject
    // default_value = "BlockIndex_StreamerMessages"
    fn subject(&self) -> String {
        return self.to_owned().subject;
    }

    /// Streaming messages format (`CBOR` or `JSON`), suffix for subject name
    // default_value = "CBOR"
    fn msg_format(&self) -> MsgFormat {
        return self.msg_format;
    }
}

/// Producer's methods for Borealis NATS Bus
pub trait Producer {
    /// root CA certificate
    fn root_cert_path(&self) -> Option<std::path::PathBuf>;

    /// client certificate
    fn client_cert_path(&self) -> Option<std::path::PathBuf>;

    /// client private key
    fn client_private_key(&self) -> Option<std::path::PathBuf>;

    /// Path to NATS credentials (JWT/NKEY tokens)
    fn creds_path(&self) -> Option<std::path::PathBuf>;

    /// Borealis Bus (NATS based MOM/MQ/SOA service bus) protocol://address:port
    /// Example: "nats://borealis.aurora.dev:4222" or "tls://borealis.aurora.dev:4443" for TLS connection
    // default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    fn nats_server(&self) -> String;

    /// Stream messages to subject
    // default_value = "BlockIndex_StreamerMessages"
    fn subject(&self) -> String;

    /// Streaming messages format (`CBOR` or `JSON`), suffix for subject name
    // default_value = "CBOR"
    fn msg_format(&self) -> MsgFormat;

    /// Create connection to Borealis NATS Bus
    fn nats_connect(&self) -> nats::Connection {
        let creds_path = self
            .creds_path()
            .unwrap_or(std::path::PathBuf::from("./.nats/seed/nats.creds"));

        let options = match (
            self.root_cert_path(),
            self.client_cert_path(),
            self.client_private_key(),
        ) {
            (Some(root_cert_path), None, None) => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [TLS, Server Auth]")
                    .tls_required(true)
                    .add_root_certificate(root_cert_path)
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(
                        || info!(target: "borealis_indexer", "connection has been reestablished"),
                    )
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1_usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt
                                * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_indexer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(
                        || info!(target: "borealis_indexer", "connection has been lost"),
                    ) // todo: re-run message producer
                    .close_callback(
                        || info!(target: "borealis_indexer", "connection has been closed"),
                    ) // todo: re-run message producer
            }
            (Some(root_cert_path), Some(client_cert_path), Some(client_private_key)) => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [TLS, Server Auth, Client Auth]")
                    .tls_required(true)
                    .add_root_certificate(root_cert_path)
                    .client_cert(client_cert_path, client_private_key)
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(
                        || info!(target: "borealis_indexer", "connection has been reestablished"),
                    )
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1_usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt
                                * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_indexer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(
                        || info!(target: "borealis_indexer", "connection has been lost"),
                    ) // todo: re-run message producer
                    .close_callback(
                        || info!(target: "borealis_indexer", "connection has been closed"),
                    ) // todo: re-run message producer
            }
            _ => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [NATS, without TLS]")
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(
                        || info!(target: "borealis_indexer", "connection has been reestablished"),
                    )
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1_usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt
                                * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_indexer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(
                        || info!(target: "borealis_indexer", "connection has been lost"),
                    ) // todo: re-run message producer
                    .close_callback(
                        || info!(target: "borealis_indexer", "connection has been closed"),
                    ) // todo: re-run message producer
            }
        };

        let nats_connection = options
            .connect(self.nats_server().as_str())
            .expect("NATS connection error or wrong credentials");

        nats_connection
    }

    /// Check connection to Borealis NATS Bus
    fn nats_check_connection(&self, nats_connection: &nats::Connection) {
        // info!(target: "borealis_indexer", "NATS Connection: {:?}", nats_connection);
        info!(target: "borealis_indexer", "round trip time (rtt) between this client and the current NATS server: {:?}", nats_connection.rtt());
        info!(target: "borealis_indexer", "this client IP address, as known by the current NATS server: {:?}", nats_connection.client_ip());
        info!(target: "borealis_indexer", "this client ID, as known by the current NATS server: {:?}", nats_connection.client_id());
        info!(target: "borealis_indexer", "maximum payload size the current NATS server will accept: {:?}", nats_connection.max_payload());
    }

    /// Create Borealis Message with payload
    fn message_encode<T: Serialize>(&self, msg_seq_id: u64, payload: &T) -> Vec<u8> {
        match self.msg_format() {
            MsgFormat::Cbor => BorealisMessage::new(msg_seq_id, payload).to_cbor().unwrap(),
            MsgFormat::Json => BorealisMessage::new(msg_seq_id, payload).to_json_bytes().unwrap(),
        }
    }

    /// Publish (transfer) message to Borealis NATS Bus
    fn message_publish<T: AsRef<[u8]>>(&self, nats_connection: &nats::Connection, message: &T) {
        nats_connection
            .publish(
                format!("{}_{}", self.subject(), self.msg_format().to_string()).as_str(),
                message,
            )
            .expect(
                format!(
                    "[Message as {} encoded bytes vector] Message passing error",
                    self.msg_format().to_string()
                )
                .as_str(),
            );
    }
}
