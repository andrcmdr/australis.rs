use actix;
use chrono::{DateTime as ChronoDateTime, Utc};
// use chrono::TimeZone;
use nats;
use nats::jetstream::{
    AccountInfo, AckPolicy, Consumer as JetStreamConsumer, ConsumerConfig, ConsumerInfo, DateTime,
    DeliverPolicy, DiscardPolicy, ReplayPolicy, RetentionPolicy, StorageType, StreamConfig,
    StreamInfo,
};
// use near_indexer::StreamerMessage;
use borealis_types::prelude::{BorealisMessage, StreamerMessage};
use serde::de::DeserializeOwned;
use serde_cbor as cbor;
use serde_json;
use tracing::info;
use tracing_subscriber::EnvFilter;

use core::str::FromStr;

pub type Error = Box<dyn std::error::Error + 'static>;

/// CLI options to run Borealis Consumer
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
    /// default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    pub nats_server: String,
    /// Consumer work mode (standard `Subscriber` or `JetStream` subscriber)
    /// default_value = "JetStream"
    pub work_mode: WorkMode,
    /// Consumer subject, for subscription and to take messages from
    /// default_value = "BlockIndex_StreamerMessages"
    pub subject: String,
    /// Consuming messages format (`CBOR` or `JSON`), suffix for subject name
    /// default_value = "CBOR"
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
            work_mode: WorkMode::Jetstream,
            subject: "BlockIndex_StreamerMessages_mainnet".to_string(),
            msg_format: MsgFormat::Cbor,
        }
    }
}

/// Consumer work mode
#[derive(Debug, Clone, Copy)]
pub enum WorkMode {
    Subscriber,
    Jetstream,
}

impl FromStr for WorkMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "subscriber" => Ok(WorkMode::Subscriber),
            "jetstream" => Ok(WorkMode::Jetstream),
            _ => Err(
                "Unknown consumer work mode: `--work-mode` should be `Subscriber` or `JetStream`"
                    .to_string()
                    .into(),
            ),
        }
    }
}

/// Consuming messages format (should be upper case, 'cause it's a suffix for `subject` name, and NATS subject is case sensitive)
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
        let input = s.to_lowercase();
        match input.as_str() {
            "0" | "withblockhashheight" => Ok(VerbosityLevel::WithBlockHashHeight),
            "1" | "withstreamermessagedump" => Ok(VerbosityLevel::WithStreamerMessageDump),
            "2" | "withstreamermessageparse" => Ok(VerbosityLevel::WithStreamerMessageParse),
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

/// Data provider methods from Context for Consumer trait
impl Consumer for Context {
    /// root CA certificate
    fn root_cert_path(&self) -> Option<std::path::PathBuf> {
        return self.to_owned().root_cert_path;
    }

    /// client certificate
    fn client_cert_path(&self) -> Option<std::path::PathBuf> {
        return  self.to_owned().client_cert_path;
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
    /// default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    fn nats_server(&self) -> String {
        return self.to_owned().nats_server;
    }

    /// Consumer work mode (standard `Subscriber` or `JetStream` subscriber)
    /// default_value = "JetStream"
    fn work_mode(&self) -> WorkMode {
        return self.work_mode;
    }

    /// Consumer subject, for subscription and to take messages from
    /// default_value = "BlockIndex_StreamerMessages"
    fn subject(&self) -> String {
        return self.to_owned().subject;
    }

    /// Consuming messages format (`CBOR` or `JSON`), suffix for subject name
    /// default_value = "CBOR"
    fn msg_format(&self) -> MsgFormat {
        return self.msg_format;
    }
}

/// Consumer's methods for Borealis NATS Bus
pub trait Consumer {
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
    /// default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    fn nats_server(&self) -> String;

    /// Consumer work mode (standard `Subscriber` or `JetStream` subscriber)
    /// default_value = "JetStream"
    fn work_mode(&self) -> WorkMode;

    /// Consumer subject, for subscription and to take messages from
    /// default_value = "BlockIndex_StreamerMessages"
    fn subject(&self) -> String;

    /// Consuming messages format (`CBOR` or `JSON`), suffix for subject name
    /// default_value = "CBOR"
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
                        || info!(target: "borealis_consumer", "connection has been reestablished"),
                    )
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1 as usize
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
                            target: "borealis_consumer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(
                        || info!(target: "borealis_consumer", "connection has been lost"),
                    ) // todo: re-run message consumer
                    .close_callback(|| info!(target: "borealis_consumer", "connection has been closed"))
                // todo: re-run message consumer
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
                        || info!(target: "borealis_consumer", "connection has been reestablished"),
                    )
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1 as usize
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
                            target: "borealis_consumer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(
                        || info!(target: "borealis_consumer", "connection has been lost"),
                    ) // todo: re-run message consumer
                    .close_callback(|| info!(target: "borealis_consumer", "connection has been closed"))
                // todo: re-run message consumer
            }
            _ => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [NATS, without TLS]")
                    .reconnect_buffer_size(1024 * 1024 * 1024)
                    .max_reconnects(100000)
                    .reconnect_callback(
                        || info!(target: "borealis_consumer", "connection has been reestablished"),
                    )
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1 as usize
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
                            target: "borealis_consumer",
                            "reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .disconnect_callback(
                        || info!(target: "borealis_consumer", "connection has been lost"),
                    ) // todo: re-run message consumer
                    .close_callback(|| info!(target: "borealis_consumer", "connection has been closed"))
                // todo: re-run message consumer
            }
        };

        let nats_connection = options
            .connect(self.nats_server().as_str())
            .expect("NATS connection error or wrong credentials");

        nats_connection
    }

    /// Check connection to Borealis NATS Bus
    fn nats_check_connection(&self, nats_connection: &nats::Connection) {
        // info!(target: "borealis_consumer", "NATS Connection: {:?}", nats_connection);
        info!(target: "borealis_consumer", "round trip time (rtt) between this client and the current NATS server: {:?}", nats_connection.rtt());
        info!(target: "borealis_consumer", "this client IP address, as known by the current NATS server: {:?}", nats_connection.client_ip());
        info!(target: "borealis_consumer", "this client ID, as known by the current NATS server: {:?}", nats_connection.client_id());
        info!(target: "borealis_consumer", "maximum payload size the current NATS server will accept: {:?}", nats_connection.max_payload());
    }

    /// Initialization for JetStream consumers
    fn init(&self) {
        let nats_connection = self.nats_connect();
        let stream_info = self.jetstream_create_stream(
            &nats_connection,
            format!("{}_{}", self.subject(), self.msg_format().to_string()),
            Some(vec![format!(
                "{}_{}",
                self.subject(),
                self.msg_format().to_string()
            )]),
        );
        let consumer = self.jetstream_create_consumer_from_context(&nats_connection);

        info!(
            target: "borealis_consumer",
            "Initialized:\nStream:\n{:?}\nConsumer:\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            stream_info,
            consumer.nc,
            consumer.stream,
            consumer.cfg,
            consumer.push_subscriber,
            consumer.timeout
        );
    }

    /// Create JetStream with initial stream name and consisting of exact subject names
    fn jetstream_create_stream(
        &self,
        nats_connection: &nats::Connection,
        stream_name: String,
        subject_names: Option<Vec<String>>,
    ) -> StreamInfo {
        // JetStreams cannot be created from NATS Client side due to restrictions on NATS server side, but this ability is still available in library for client side consumers
        let stream_info = nats_connection.create_stream(StreamConfig {
            name: stream_name,
            discard: DiscardPolicy::Old,
            subjects: subject_names,
            duplicate_window: 86400,
            retention: RetentionPolicy::Limits,
            storage: StorageType::File,
            ..Default::default()
        }).expect("IO error, something went wrong while creating a new stream, maybe stream already exist");

        info!(
            target: "borealis_consumer",
            "Initialized:\nStream:\n{:?}",
            stream_info
        );

        stream_info
    }

    /// Create JetStream consumer with custom parameters
    /// (see NATS documentation descrption for the meaning of particular ConsumerConfig parameters)
    fn jetstream_create_consumer(
        &self,
        nats_connection: &nats::Connection,
        stream_name: String,
        deliver_subject: Option<String>,
        durable_name: Option<String>,
        filter_subject: String,
    ) -> JetStreamConsumer {
        let consumer = JetStreamConsumer::create_or_open(nats_connection.to_owned(), stream_name.as_str(), ConsumerConfig {
            deliver_subject,
            durable_name,
            deliver_policy: DeliverPolicy::Last,
            ack_policy: AckPolicy::Explicit,
            filter_subject,
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        }).expect("IO error, something went wrong while creating a new consumer or returning an existent consumer");

        info!(
            target: "borealis_consumer",
            "Initialized:\nConsumer:\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            consumer.nc,
            consumer.stream,
            consumer.cfg,
            consumer.push_subscriber,
            consumer.timeout
        );

        consumer
    }

    /// Create JetStream consumer to consume messages after message with exact sequential number
    fn jetstream_create_consumer_from_start_seq(
        &self,
        nats_connection: &nats::Connection,
        stream_name: String,
        deliver_subject: Option<String>,
        durable_name: Option<String>,
        filter_subject: String,
        start_seq: Option<i64>,
    ) -> JetStreamConsumer {
        let consumer = JetStreamConsumer::create_or_open(nats_connection.to_owned(), stream_name.as_str(), ConsumerConfig {
            deliver_subject,
            durable_name,
            deliver_policy: DeliverPolicy::ByStartSeq,
            ack_policy: AckPolicy::Explicit,
            filter_subject,
            replay_policy: ReplayPolicy::Instant,
            opt_start_seq: match start_seq {
                Some(seq) => seq,
                None => Default::default(),
            },
            ..Default::default()
        }).expect("IO error, something went wrong while creating a new consumer or returning an existent consumer");

        info!(
            target: "borealis_consumer",
            "Initialized:\nConsumer:\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            consumer.nc,
            consumer.stream,
            consumer.cfg,
            consumer.push_subscriber,
            consumer.timeout
        );

        consumer
    }

    /// Create JetStream consumer to consume messages after message with exact timestamp
    fn jetstream_create_consumer_from_start_time(
        &self,
        nats_connection: &nats::Connection,
        stream_name: String,
        deliver_subject: Option<String>,
        durable_name: Option<String>,
        filter_subject: String,
        start_time: Option<ChronoDateTime<Utc>>,
    ) -> JetStreamConsumer {
        let consumer = JetStreamConsumer::create_or_open(nats_connection.to_owned(), stream_name.as_str(), ConsumerConfig {
            deliver_subject,
            durable_name,
            deliver_policy: DeliverPolicy::ByStartTime,
            ack_policy: AckPolicy::Explicit,
            filter_subject,
            replay_policy: ReplayPolicy::Instant,
            opt_start_time: match start_time {
                Some(time) => Some(DateTime(time)),
                None => Default::default(),
            },
            ..Default::default()
        }).expect("IO error, something went wrong while creating a new consumer or returning an existent consumer");

        info!(
            target: "borealis_consumer",
            "Initialized:\nConsumer:\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            consumer.nc,
            consumer.stream,
            consumer.cfg,
            consumer.push_subscriber,
            consumer.timeout
        );

        consumer
    }

    /// Create JetStream consumer from Context parameters
    /// (see documentation descrption for the meaning of particular Context and ConsumerConfig parameters)
    fn jetstream_create_consumer_from_context(
        &self,
        nats_connection: &nats::Connection,
    ) -> JetStreamConsumer {
        let consumer = JetStreamConsumer::create_or_open(nats_connection.to_owned(), format!("{}_{}", self.subject(), self.msg_format().to_string()).as_str(), ConsumerConfig {
            deliver_subject: Some(format!("{}_{}", self.subject(), self.msg_format().to_string())),
            durable_name: Some(format!("Borealis_Consumer_{}_{}", self.subject(), self.msg_format().to_string())),
            deliver_policy: DeliverPolicy::Last,
            ack_policy: AckPolicy::Explicit,
            filter_subject: format!("{}_{}", self.subject(), self.msg_format().to_string()),
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        }).expect("IO error, something went wrong while creating a new consumer or returning an existent consumer");

        info!(
            target: "borealis_consumer",
            "Initialized:\nConsumer:\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            consumer.nc,
            consumer.stream,
            consumer.cfg,
            consumer.push_subscriber,
            consumer.timeout
        );

        consumer
    }

    /// Get names for all JetStream streams from NATS server
    fn get_stream_names(&self, nats_connection: &nats::Connection) {
        nats_connection
            .stream_names()
            .for_each(|stream_name| match stream_name {
                Ok(stream_name) => {
                    info!(
                        target: "borealis_consumer",
                        "Stream name: {}",
                        stream_name
                    );
                }
                Err(error) => {
                    info!(
                        target: "borealis_consumer",
                        "Error during retrieving a stream name: {}",
                        error
                    );
                }
            });
    }

    /// Get full information about all JetStream streams from NATS server
    fn get_streams_list(&self, nats_connection: &nats::Connection) {
        nats_connection
            .list_streams()
            .for_each(|stream_info| match stream_info {
                Ok(stream_info) => {
                    info!(
                        target: "borealis_consumer",
                        "Stream information: {:?}",
                        stream_info
                    );
                }
                Err(error) => {
                    info!(
                        target: "borealis_consumer",
                        "Error during retrieving a stream information: {}",
                        error
                    );
                }
            });
    }

    /// Get full information about particular JetStream stream from NATS server
    fn get_stream_info(
        &self,
        nats_connection: &nats::Connection,
        stream_name: String,
    ) -> std::io::Result<StreamInfo> {
        match nats_connection.stream_info(stream_name.as_str()) {
            Ok(stream_info) => {
                info!(
                    target: "borealis_consumer",
                    "Stream {} information: {:?}",
                    stream_name,
                    stream_info
                );
                Ok(stream_info)
            }
            Err(error) => {
                info!(
                    target: "borealis_consumer",
                    "Error during retrieving a stream {} information: {}",
                    stream_name,
                    error
                );
                Err(error)
            }
        }
    }

    /// Get full information about all JetStream consumers created for particular stream from NATS server
    fn get_consumers_list(&self, nats_connection: &nats::Connection, stream_name: String) {
        match nats_connection.list_consumers(stream_name.as_str()) {
            Ok(consumers_list) => {
                consumers_list.for_each(|consumer_info| {
                    if let Ok(consumer_info) = consumer_info {
                        info!(
                            target: "borealis_consumer",
                            "Consumer information for stream {}: {:?}",
                            stream_name,
                            consumer_info
                        );
                    } else if let Err(error) = consumer_info {
                        info!(
                            target: "borealis_consumer",
                            "Error during retrieving a consumer information for stream {}: {}",
                            stream_name,
                            error
                        );
                    };
                });
            }
            Err(error) => {
                info!(
                    target: "borealis_consumer",
                    "Error during retrieving a consumer list for stream {}: {}",
                    stream_name,
                    error
                );
            }
        }
    }

    /// Get full information about certain JetStream consumer created for particular stream from NATS server
    fn get_consumer_info(
        &self,
        nats_connection: &nats::Connection,
        stream_name: String,
        consumer_name: String,
    ) -> std::io::Result<ConsumerInfo> {
        match nats_connection.consumer_info(stream_name.as_str(), consumer_name.as_str()) {
            Ok(consumer_info) => {
                info!(
                    target: "borealis_consumer",
                    "Consumer {} information for stream {}: {:?}",
                    consumer_name,
                    stream_name,
                    consumer_info
                );
                Ok(consumer_info)
            }
            Err(error) => {
                info!(
                    target: "borealis_consumer",
                    "Error during retrieving a consumer {} information for stream {}: {}",
                    consumer_name,
                    stream_name,
                    error
                );
                Err(error)
            }
        }
    }

    /// Get full information about JetStream client account from NATS server
    fn get_jetstream_account_info(
        &self,
        nats_connection: &nats::Connection,
    ) -> std::io::Result<AccountInfo> {
        match nats_connection.account_info() {
            Ok(account_info) => {
                info!(
                    target: "borealis_consumer",
                    "JetStream account information: {:?}",
                    account_info
                );
                Ok(account_info)
            }
            Err(error) => {
                info!(
                    target: "borealis_consumer",
                    "Error during retrieving a JetStream account information: {}",
                    error
                );
                Err(error)
            }
        }
    }

    /// Create subscription to NATS subject
    fn create_subscription(
        &self,
        nats_connection: &nats::Connection,
        subject_name: String,
    ) -> nats::Subscription {
        let subscription = nats_connection
            .subscribe(subject_name.as_str())
            .expect("Subscription error: maybe wrong or nonexistent `subject` name");
        subscription
    }

    /// Create subscription to NATS subject from Context parameters
    /// (see documentation descrption for the meaning of particular Context parameters)
    fn create_subscription_from_context(
        &self,
        nats_connection: &nats::Connection,
    ) -> nats::Subscription {
        let subscription = nats_connection
            .subscribe(format!("{}_{}", self.subject(), self.msg_format().to_string()).as_str())
            .expect("Subscription error: maybe wrong or nonexistent `subject` name");
        subscription
    }

    /// Run Borealis Consumer with messages listener for Borealis NATS Bus
    fn run(&self) {
        let nats_connection = self.nats_connect();
        let system = actix::System::new();
        system.block_on(async move {
            self.listen_messages(&nats_connection);
        });
        system.run().unwrap();
    }

    /// Listen NATS messages from particular subject subscription
    /// or from JetStream stream with parameters set in Context type
    fn listen_messages(&self, nats_connection: &nats::Connection) {
        match self.work_mode() {
            WorkMode::Subscriber => {
                let subscription = self.create_subscription_from_context(&nats_connection);
                let timeout = std::time::Duration::from_millis(10000);
                loop {
                    info!(
                        target: "borealis_consumer",
                        "Message consumer loop started: listening for new messages\n"
                    );
                    if let Ok(msg) = subscription.next_timeout(timeout)
                    {
                        info!(target: "borealis_consumer", "Received message:\n{}", &msg);
                        self.handle_message(msg);
                    } else {
                        info!(
                            target: "borealis_consumer",
                            "Message wasn't received within {:?} timeframe: Error occured due to waiting timeout for message receiving was elapsed\n",
                            timeout
                        );
                    };
                }
            }
            WorkMode::Jetstream => {
                let mut consumer = self.jetstream_create_consumer_from_context(&nats_connection);
                consumer.timeout = std::time::Duration::from_millis(10000);
                loop {
                    info!(
                        target: "borealis_consumer",
                        "Message JetStream consumer loop started: listening for new messages\n"
                    );
                    if let Ok(message) = consumer.process_timeout(|msg| {
                        info!(target: "borealis_consumer", "Received message:\n{}", msg);
                        Ok(msg.to_owned())
                    }) {
                        self.handle_message(message);
                    } else {
                        info!(
                            target: "borealis_consumer",
                            "Message wasn't received within {:?} timeframe: Error occured due to waiting timeout for message receiving was elapsed\n",
                            consumer.timeout
                        );
                    };
                }
            }
        }
    }

    /// Handle received NATS message
    /// (decode, extract `StreamerMessage` as payload, dump information from `StreamerMessage`)
    fn handle_message(&self, msg: nats::Message) {
        info!(
            target: "borealis_consumer",
            "Message consumer loop executed: message received\n"
        );

        // Decoding of Borealis Message receved from NATS subject/jetstream
        let borealis_message: BorealisMessage<StreamerMessage> = self.message_decode(msg);
        // Get `StreamerMessage` from received Borealis Message
        let streamer_message: StreamerMessage = borealis_message.payload;
        self.message_dump(Some(VerbosityLevel::WithBlockHashHeight), streamer_message);
    }

    /// Handle received NATS message
    /// (with decoding and extraction of `StreamerMessage` as payload, dump information from `StreamerMessage`)
    fn handle_streamer_message(&self, msg: nats::Message) {
        info!(
            target: "borealis_consumer",
            "Message consumer loop executed: message received\n"
        );

        // Get `StreamerMessage` from Borealis Message receved from NATS subject/jetstream
        let streamer_message: StreamerMessage = self.message_get_payload(msg);
        self.message_dump(Some(VerbosityLevel::WithBlockHashHeight), streamer_message);
    }

    /// Extract `StreamerMessage` as payload from received NATS message
    fn message_get_payload<T: DeserializeOwned>(&self, msg: nats::Message) -> T {
        // Decoding of Borealis Message receved from NATS subject/jetstream
        let borealis_message: BorealisMessage<T> = self.message_decode(msg);
        // Get `StreamerMessage` from received Borealis Message
        let payload: T = borealis_message.payload;
        payload
    }

    /// Decode received NATS message from CBOR (of JSON)
    fn message_decode<T: DeserializeOwned>(
        &self,
        msg: nats::Message,
    ) -> BorealisMessage<T> {
        // Decoding of Borealis Message receved from NATS subject/jetstream
        let borealis_message: BorealisMessage<T> = match self.msg_format() {
            MsgFormat::Cbor => BorealisMessage::from_cbor(msg.data.as_ref())
                .expect("[From CBOR bytes vector: message empty] Message decoding error"),
            MsgFormat::Json => BorealisMessage::from_json_bytes(msg.data.as_ref())
                .expect("[From JSON bytes vector: message empty] Message decoding error"),
        };
        borealis_message
    }

    /// Dump information from `StreamerMessage` payload, extracted from received NATS message
    fn message_dump(&self, verbosity_level: Option<VerbosityLevel>, streamer_message: StreamerMessage) {
        // Data handling from `StreamerMessage` data structure. For custom filtering purposes.
        // Same as: jq '{block_height: .block.header.height, block_hash: .block.header.hash, block_header_chunk: .block.chunks[0], shard_chunk_header: .shards[0].chunk.header, transactions: .shards[0].chunk.transactions, receipts: .shards[0].chunk.receipts, receipt_execution_outcomes: .shards[0].receipt_execution_outcomes, state_changes: .state_changes}'

        info!(
            target: "borealis_consumer",
            "block_height: #{}, block_hash: {}\n",
            &streamer_message.block.header.height,
            &streamer_message.block.header.hash
        );

        if let Some(_verbosity_level) = verbosity_level {
            println!(
                "block_height: #{}, block_hash: {}\n",
                &streamer_message.block.header.height, &streamer_message.block.header.hash
            );
        };

        if let Some(VerbosityLevel::WithStreamerMessageDump)
        | Some(VerbosityLevel::WithStreamerMessageParse) = verbosity_level
        {
            println!(
                "streamer_message: {}\n",
                serde_json::to_string_pretty(&streamer_message).unwrap()
            );
            println!(
                "streamer_message: {}\n",
                serde_json::to_string(&streamer_message).unwrap()
            );
        };

        if let Some(VerbosityLevel::WithStreamerMessageParse) = verbosity_level {
            println!(
                "streamer_message: {}\n",
                serde_json::to_value(&streamer_message).unwrap()
            );
            println!(
                "streamer_message: {:?}\n",
                cbor::to_vec(&streamer_message).unwrap()
            );

            println!(
                "block_header: {}\n",
                serde_json::to_value(&streamer_message.block.header).unwrap()
            );
            println!(
                "block_header: {:?}\n",
                cbor::to_vec(&streamer_message.block.header).unwrap()
            );

            println!(
                "block_header_chunks#: {}\n",
                streamer_message.block.chunks.len()
            );
            streamer_message.block.chunks.iter().for_each(|chunk| {
                println!(
                    "block_header_chunk: {}\n",
                    serde_json::to_value(&chunk).unwrap()
                );
                println!("block_header_chunk: {:?}\n", cbor::to_vec(&chunk).unwrap());
            });

            println!("shards#: {}\n", streamer_message.shards.len());
            streamer_message.shards.iter().for_each(|shard| {
                if let Some(chunk) = &shard.chunk {
                    println!(
                        "shard_chunk_header: {}\n",
                        serde_json::to_value(&chunk.header).unwrap()
                    );
                    println!(
                        "shard_chunk_header: {:?}\n",
                        cbor::to_vec(&chunk.header).unwrap()
                    );

                    println!("shard_chunk_transactions#: {}\n", chunk.transactions.len());
                    println!(
                        "shard_chunk_transactions: {}\n",
                        serde_json::to_value(&chunk.transactions).unwrap()
                    );
                    println!(
                        "shard_chunk_transactions: {:?}\n",
                        cbor::to_vec(&chunk.transactions).unwrap()
                    );

                    println!("shard_chunk_receipts#: {}\n", chunk.receipts.len());
                    println!(
                        "shard_chunk_receipts: {}\n",
                        serde_json::to_value(&chunk.receipts).unwrap()
                    );
                    println!(
                        "shard_chunk_receipts: {:?}\n",
                        cbor::to_vec(&chunk.receipts).unwrap()
                    );
                } else {
                    println!("shard_chunk_header: None\n");

                    println!("shard_chunk_transactions#: None\n");
                    println!("shard_chunk_transactions: None\n");

                    println!("shard_chunk_receipts#: None\n");
                    println!("shard_chunk_receipts: None\n");
                };

                println!(
                    "shard_receipt_execution_outcomes#: {}\n",
                    shard.receipt_execution_outcomes.len()
                );
                println!(
                    "shard_receipt_execution_outcomes: {}\n",
                    serde_json::to_value(&shard.receipt_execution_outcomes).unwrap()
                );
                println!(
                    "shard_receipt_execution_outcomes: {:?}\n",
                    cbor::to_vec(&shard.receipt_execution_outcomes).unwrap()
                );
            });

            println!("StateChanges#: {}\n", streamer_message.state_changes.len());
            streamer_message
                .state_changes
                .iter()
                .for_each(|state_change| {
                    println!(
                        "StateChange: {}\n",
                        serde_json::to_value(&state_change).unwrap()
                    );
                    println!("StateChange: {:?}\n", cbor::to_vec(&state_change).unwrap());
                });
        };
    }
}
