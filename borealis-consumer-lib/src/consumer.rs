use nats;
use tracing::info;
use tracing_subscriber::EnvFilter;

use core::str::FromStr;

type Error = Box<dyn std::error::Error + 'static>;

/// CLI options to run Borealis Consumer
#[derive(Debug, Clone)]
pub(crate) struct RunArgs {
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
//  default_value = "tls://westcoast.nats.backend.aurora.dev:4222,tls://eastcoast.nats.backend.aurora.dev:4222"
    pub nats_server: String,
    /// Consumer work mode (standard `Subscriber` or `JetStream` subscriber)
//  default_value = "JetStream"
    pub work_mode: WorkMode,
    /// Consumer subject, for subscription and to take messages from
//  default_value = "BlockIndex_StreamerMessages"
    pub subject: String,
    /// Consuming messages format (`CBOR` or `JSON`), suffix for subject name
//  default_value = "CBOR"
    pub msg_format: MsgFormat,
}

impl Default for RunArgs {
    fn default() -> Self {
        Self
    }
}

/// Consumer work mode
#[derive(Debug, Clone, Copy)]
pub(crate) enum WorkMode {
    Subscriber,
    Jetstream,
}

impl FromStr for WorkMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Subscriber" | "subscriber" => Ok(WorkMode::Subscriber),
            "JetStream" | "Jetstream" | "jetstream" => Ok(WorkMode::Jetstream),
            _ => Err("Unknown consumer work mode: `--work-mode` should be `Subscriber` or `JetStream`".to_string().into()),
        }
    }
}

/// Consuming messages format (should be upper case, 'cause it's a suffix for `subject` name, and NATS subject is case sensitive)
#[derive(Debug, Clone, Copy)]
pub(crate) enum MsgFormat {
    CBOR,
    JSON,
}

impl FromStr for MsgFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "CBOR" | "Cbor" | "cbor" => Ok(MsgFormat::CBOR),
            "JSON" | "Json" | "json" => Ok(MsgFormat::JSON),
            _ => Err("Unknown message format: `--msg-fomat` should contain `CBOR` or `JSON`".to_string().into()),
        }
    }
}

/// Verbosity level for messages dump to log and stdout:
/// WithBlockHashHeight - output only block height & hash
/// WithStreamerMessageDump - full dump of `StreamerMessage`
/// WithStreamerMessageParse - full dump with full parse of `StreamerMessage`
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum VerbosityLevel {
    WithBlockHashHeight,
    WithStreamerMessageDump,
    WithStreamerMessageParse,
}

impl FromStr for VerbosityLevel {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0" | "WithBlockHashHeight" | "Withblockhashheight" | "withblockhashheight" => Ok(VerbosityLevel::WithBlockHashHeight),
            "1" | "WithStreamerMessageDump" | "Withstreamermessagedump" | "withstreamermessagedump" => Ok(VerbosityLevel::WithStreamerMessageDump),
            "2" | "WithStreamerMessageParse" | "Withstreamermessageparse" | "withstreamermessageparse" => Ok(VerbosityLevel::WithStreamerMessageParse),
            _ => Err("Unknown output verbosity level: `--verbose` should be `WithBlockHashHeight` (`0`), `WithStreamerMessageDump` (`1`) or `WithStreamerMessageParse` (`2`)".to_string().into()),
        }
    }
}

/// Initialize logging
pub fn init_logging() {
    // Custom filters
    let env_filter = EnvFilter::new(
        "borealis-indexer=info,tokio_reactor=info,near=info,near=error,stats=info,telemetry=info,borealis_indexer=info,indexer=info,near-performance-metrics=info",
    );
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

/*
impl From/Into<cli::RunArgs> for lib::RunArgs
*/

pub trait Consumer {
    fn nats_connect(&self) -> nats::Connection;
    fn nats_check_connection(&self, nats_connection: nats::Connection);
}

impl Consumer for RunArgs {
    /// Create connection to Borealis NATS Bus
    fn nats_connect(&self) -> nats::Connection {
        let creds_path = self
            .creds_path
            .unwrap_or(std::path::PathBuf::from("./.nats/seed/nats.creds"));

        let options = match (
            self.root_cert_path,
            self.client_cert_path,
            self.client_private_key,
        ) {
            (Some(root_cert_path), None, None) => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [TLS, Server Auth]")
                    .tls_required(true)
                    .add_root_certificate(root_cert_path)
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(|| info!(target: "borealis_consumer", "connection has been reestablished"))
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1 as usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_consumer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(|| info!(target: "borealis_consumer", "connection has been lost")) // todo: re-run message consumer
                    .close_callback(|| info!(target: "borealis_consumer", "connection has been closed")) // todo: re-run message consumer
            }
            (Some(root_cert_path), Some(client_cert_path), Some(client_private_key)) => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [TLS, Server Auth, Client Auth]")
                    .tls_required(true)
                    .add_root_certificate(root_cert_path)
                    .client_cert(client_cert_path, client_private_key)
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(|| info!(target: "borealis_consumer", "connection has been reestablished"))
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1 as usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_consumer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(|| info!(target: "borealis_consumer", "connection has been lost")) // todo: re-run message consumer
                    .close_callback(|| info!(target: "borealis_consumer", "connection has been closed")) // todo: re-run message consumer
            }
            _ => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [NATS, without TLS]")
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(|| info!(target: "borealis_consumer", "connection has been reestablished"))
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1 as usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_consumer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(|| info!(target: "borealis_consumer", "connection has been lost")) // todo: re-run message consumer
                    .close_callback(|| info!(target: "borealis_consumer", "connection has been closed")) // todo: re-run message consumer
            }
        };

        let nats_connection = options
            .connect(self.nats_server.as_str())
            .expect("NATS connection error or wrong credentials");

        nats_connection
    }

    /// Check connection to Borealis NATS Bus
    fn nats_check_connection(&self, nats_connection: nats::Connection) {
    //  info!(target: "borealis_consumer", "NATS Connection: {:?}", nats_connection);
        info!(target: "borealis_consumer", "round trip time (rtt) between this client and the current NATS server: {:?}", nats_connection.rtt());
        info!(target: "borealis_consumer", "this client IP address, as known by the current NATS server: {:?}", nats_connection.client_ip());
        info!(target: "borealis_consumer", "this client ID, as known by the current NATS server: {:?}", nats_connection.client_id());
        info!(target: "borealis_consumer", "maximum payload size the current NATS server will accept: {:?}", nats_connection.max_payload());
    }
}


/*
nats_connect(run_args) -> Connection
nats_check_connection(run_args):
let nc = nats::connect("demo.nats.io")?;
let nats_connection = nats_connect(run_args);
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.rtt
println!("server rtt: {:?}", nc.rtt());
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.client_ip
println!("ip: {:?}", nc.client_ip());
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.client_id
println!("id: {:?}", nc.client_id());
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.max_payload
println!("max payload: {:?}", nc.max_payload());


consumer_init(run_args(stream, subject, msg_format))
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.stream_names
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.list_streams
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.stream_info
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.list_consumers
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.consumer_info
https://docs.rs/nats/0.16.0/nats/struct.Connection.html#method.account_info
create_jet_stream(stream_name, subjects[subject, msg_format])
create_jet_stream_consumer(stream, subject, msg_format)
create_jet_stream_consumer_with_start_seq(stream, subject, msg_format, start_seq)
create_jet_stream_consumer_with_start_time(stream, subject, msg_format, start_time)

consumer_run(work_mode, subject, msg_format)
consumer_subscribe(subject, msg_format) -> subscription
| jet_stream_consumer_create_or_open(stream, subject, msg_format) -> nats::jetstream::Consumer

message_consumer(subscription, msg_format)
| message_jetstream_consumer(consumer, msg_format)
message_decode(msg, msg_format) -> RawEvent, Headers, StreamerMessage


message_dump/message_print(StreamerMessage)
message_log(StreamerMessage)
*/

