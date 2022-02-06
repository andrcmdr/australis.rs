use actix;
use borealis_types::prelude::BorealisMessage;
use nats;
use near_indexer;
use serde::Serialize;
use serde_cbor as cbor;
use serde_json;
use tokio::sync::mpsc;
use tokio_stream;
use futures::StreamExt;
use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use near_indexer::near_primitives::types::Gas;

use core::str::FromStr;

pub type Error = Box<dyn std::error::Error + 'static>;

/// Options to run NATS Producer and Borealis Indexer
#[derive(Debug, Clone)]
pub struct RunArgs {
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
//  default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    pub nats_server: String,
    /// Stream messages to subject
//  default_value = "BlockIndex_StreamerMessages"
    pub subject: String,
    /// Streaming messages format (`CBOR` or `JSON`), suffix for subject name
//  default_value = "CBOR"
    pub msg_format: MsgFormat,
//  default_value = "FromInterruption"
    pub sync_mode: SyncMode,
    pub block_height: Option<u64>,
//  default_value = "StreamWhileSyncing"
    pub await_synced: AwaitSynced,
}

impl Default for RunArgs {
    fn default() -> Self {
        Self {
            root_cert_path: Some(std::path::PathBuf::from("./.nats/seed/root-ca.crt")),
            client_cert_path: None,
            client_private_key: None,
            creds_path: Some(std::path::PathBuf::from("./.nats/seed/nats.creds")),
            nats_server: "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222".to_string(),
            subject: "BlockIndex_StreamerMessages_mainnet".to_string(),
            msg_format: MsgFormat::CBOR,
            sync_mode: SyncMode::LatestSynced,
            block_height: None,
            await_synced: AwaitSynced::WaitForFullSync,
        }
    }
}

/// Streaming messages format (should be upper case, 'cause it's a suffix for `subject` name, and NATS subject is case sensitive)
#[derive(Debug, Clone, Copy)]
pub enum MsgFormat {
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

/// Definition of a syncing mode for NEAR Indexer
#[derive(Debug, Clone, Copy)]
pub enum SyncMode {
    /// Real-time syncing, always taking the latest finalized block to stream
    LatestSynced,
    /// Starts syncing from the block NEAR Indexer was interrupted last time
    FromInterruption,
    /// Specific block height to start syncing from, RunArgs.block_height should follow after it
    BlockHeight,
}

impl FromStr for SyncMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "LatestSynced" | "Latestsynced" | "latestsynced" => Ok(SyncMode::LatestSynced),
            "FromInterruption" | "Frominterruption" | "frominterruption" => Ok(SyncMode::FromInterruption),
            "BlockHeight" | "Blockheight" | "blockheight" => Ok(SyncMode::BlockHeight),
            _ => Err("Unknown indexer synchronization mode: `--sync-mode` should be `LatestSynced`, `FromInterruption` or `BlockHeight` with --block-height explicit pointing".to_string().into()),
        }
    }
}

/// Define whether await for node to be fully synced or stream while syncing (useful for indexing from genesis)
#[derive(Debug, Clone, Copy)]
pub enum AwaitSynced {
    /// Don't stream until the node is fully synced
    WaitForFullSync,
    /// Stream while node is syncing
    StreamWhileSyncing,
}

impl FromStr for AwaitSynced {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "WaitForFullSync" | "Waitforfullsync" | "waitforfullsync" => Ok(AwaitSynced::WaitForFullSync),
            "StreamWhileSyncing" | "Streamwhilesyncing" | "streamwhilesyncing" => Ok(AwaitSynced::StreamWhileSyncing),
            _ => Err("Unknown indexer node await synchronization mode: `--await-synced` should be `WaitForFullSync` or `StreamWhileSyncing`".to_string().into()),
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

/// Override standard config args with library's `InitConfigArgs` type options
#[derive(Debug, Clone)]
pub struct InitConfigArgs {
    /// chain/network id (localnet, devnet, testnet, betanet, mainnet)
    pub chain_id: Option<String>,
    /// Account ID for the validator key
    pub account_id: Option<String>,
    /// Specify private key generated from seed (TESTING ONLY)
    pub test_seed: Option<String>,
    /// Number of shards to initialize the chain with
//  default_value = "1"
    pub num_shards: u64,
    /// Makes block production fast (TESTING ONLY)
    pub fast: bool,
    /// Genesis file to use when initialize testnet (including downloading)
    pub genesis: Option<String>,
    /// Download the verified NEAR genesis file automatically.
    pub download_genesis: bool,
    /// Specify a custom download URL for the genesis-file.
    pub download_genesis_url: Option<String>,
    /// Download the verified NEAR config file automatically.
    pub download_config: bool,
    /// Specify a custom download URL for the config file.
    pub download_config_url: Option<String>,
    /// Specify the boot nodes to bootstrap the network
    pub boot_nodes: Option<String>,
    /// Specify a custom max_gas_burnt_view limit.
    pub max_gas_burnt_view: Option<Gas>,
}

impl Default for InitConfigArgs {
    fn default() -> Self {
        Self {
            chain_id: Some("mainnet".to_string()),
            account_id: Some("borealis.indexer.near".to_string()),
            test_seed: None,
            num_shards: 1,
            fast: false,
            genesis: Some("genesis.json".to_string()),
            download_genesis: true,
            download_genesis_url: Some("https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/mainnet/genesis.json".to_string()),
            download_config: true,
            download_config_url: Some("https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/mainnet/config.json".to_string()),
            boot_nodes: Some("ed25519:86EtEy7epneKyrcJwSWP7zsisTkfDRH5CFVszt4qiQYw@35.195.32.249:24567,ed25519:BFB78VTDBBfCY4jCP99zWxhXUcFAZqR22oSx2KEr8UM1@35.229.222.235:24567,ed25519:Cw1YyiX9cybvz3yZcbYdG7oDV6D7Eihdfc8eM1e1KKoh@35.195.27.104:24567,ed25519:33g3PZRdDvzdRpRpFRZLyscJdbMxUA3j3Rf2ktSYwwF8@34.94.132.112:24567,ed25519:CDQFcD9bHUWdc31rDfRi4ZrJczxg8derCzybcac142tK@35.196.209.192:24567".to_string()),
            max_gas_burnt_view: None,
        }
    }
}

/// Override standard config args with library's `InitConfigArgs` type options
impl From<InitConfigArgs> for near_indexer::InitConfigArgs {
    fn from(config_args: InitConfigArgs) -> Self {
        Self {
            chain_id: config_args.chain_id,
            account_id: config_args.account_id,
            test_seed: config_args.test_seed,
            num_shards: config_args.num_shards,
            fast: config_args.fast,
            genesis: config_args.genesis,
            download_genesis: config_args.download_genesis,
            download_genesis_url: config_args.download_genesis_url,
            download_config: config_args.download_config,
            download_config_url: config_args.download_config_url,
            boot_nodes: config_args.boot_nodes,
            max_gas_burnt_view: config_args.max_gas_burnt_view,
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
        .with_writer(std::io::stdout)
        .init();
}

/// Indexer's methods for Indexer as Producer for Borealis NATS Bus

/// Initialize Indexer's configurations
pub fn init(args: InitConfigArgs, home_path: Option<std::path::PathBuf>) {
    // let home_dir = home_path.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));
    let home_dir = home_path
        .unwrap_or(std::path::PathBuf::from("./.borealis-indexer"));
    
    near_indexer::indexer_init_configs(&home_dir, args.into()).expect("Error while creating Indexer's initial configuration files");
}

/// Producer's methods for Borealis NATS Bus

/// Create connection to Borealis NATS Bus
pub fn nats_connect(args: RunArgs) -> nats::Connection {
    let creds_path = args
        .creds_path
        .unwrap_or(std::path::PathBuf::from("./.nats/seed/nats.creds"));

    let options = match (
        args.root_cert_path,
        args.client_cert_path,
        args.client_private_key,
    ) {
        (Some(root_cert_path), None, None) => {
            nats::Options::with_credentials(creds_path)
                .with_name("Borealis Indexer [TLS, Server Auth]")
                .tls_required(true)
                .add_root_certificate(root_cert_path)
                .reconnect_buffer_size(1024 * 1024 * 1024)
                .max_reconnects(100000)
                .reconnect_callback(|| info!(target: "borealis_indexer", "connection has been reestablished"))
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
                        target: "borealis_indexer",
                        "reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .disconnect_callback(|| info!(target: "borealis_indexer", "connection has been lost")) // todo: re-run message producer
                .close_callback(|| info!(target: "borealis_indexer", "connection has been closed")) // todo: re-run message producer
        }
        (Some(root_cert_path), Some(client_cert_path), Some(client_private_key)) => {
            nats::Options::with_credentials(creds_path)
                .with_name("Borealis Indexer [TLS, Server Auth, Client Auth]")
                .tls_required(true)
                .add_root_certificate(root_cert_path)
                .client_cert(client_cert_path, client_private_key)
                .reconnect_buffer_size(1024 * 1024 * 1024)
                .max_reconnects(100000)
                .reconnect_callback(|| info!(target: "borealis_indexer", "connection has been reestablished"))
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
                        target: "borealis_indexer",
                        "reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .disconnect_callback(|| info!(target: "borealis_indexer", "connection has been lost")) // todo: re-run message producer
                .close_callback(|| info!(target: "borealis_indexer", "connection has been closed")) // todo: re-run message producer
        }
        _ => {
            nats::Options::with_credentials(creds_path)
                .with_name("Borealis Indexer [NATS, without TLS]")
                .reconnect_buffer_size(1024 * 1024 * 1024)
                .max_reconnects(100000)
                .reconnect_callback(|| info!(target: "borealis_indexer", "connection has been reestablished"))
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
                        target: "borealis_indexer",
                        "reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .disconnect_callback(|| info!(target: "borealis_indexer", "connection has been lost")) // todo: re-run message producer
                .close_callback(|| info!(target: "borealis_indexer", "connection has been closed")) // todo: re-run message producer
        }
    };

    let nats_connection = options
        .connect(args.nats_server.as_str())
        .expect("NATS connection error or wrong credentials");

    nats_connection
}

/// Check connection to Borealis NATS Bus
pub fn nats_check_connection(nats_connection: &nats::Connection) {
//  info!(target: "borealis_indexer", "NATS Connection: {:?}", nats_connection);
    info!(target: "borealis_indexer", "round trip time (rtt) between this client and the current NATS server: {:?}", nats_connection.rtt());
    info!(target: "borealis_indexer", "this client IP address, as known by the current NATS server: {:?}", nats_connection.client_ip());
    info!(target: "borealis_indexer", "this client ID, as known by the current NATS server: {:?}", nats_connection.client_id());
    info!(target: "borealis_indexer", "maximum payload size the current NATS server will accept: {:?}", nats_connection.max_payload());
}

/// Create Borealis Message with payload
pub fn message_encode<T: Serialize>(args: &RunArgs, msg_seq_id: u64, payload: &T) -> Vec<u8> {
    match args.msg_format {
        MsgFormat::CBOR => {
            BorealisMessage::new(
                msg_seq_id,
                payload,
            ).to_cbor()
        }
        MsgFormat::JSON => {
            BorealisMessage::new(
                msg_seq_id,
                payload,
            ).to_json_bytes()
        }
    }
}

/// Publish (transfer) message to Borealis NATS Bus
pub fn message_publish<T: AsRef<[u8]>>(args: &RunArgs, nats_connection: &nats::Connection, message: &T) {
    nats_connection.publish(
        format!("{}_{:?}", args.subject, args.msg_format).as_str(),
        message,
    )
    .expect(format!("[Message as {:?} encoded bytes vector] Message passing error", args.msg_format).as_str());
}

///Run Borealis Indexer as Borealis NATS Bus Producer
pub fn run(args: &RunArgs, home_path: Option<std::path::PathBuf>) {
    // let home_dir = home_path.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));
    let home_dir = home_path
        .unwrap_or(std::path::PathBuf::from("./.borealis-indexer"));

    let indexer_config = near_indexer::IndexerConfig {
        home_dir,
        // recover and continue message streaming from latest synced block (real-time), or from interruption, or from exact block height
        sync_mode: match args.sync_mode {
            SyncMode::LatestSynced => near_indexer::SyncModeEnum::LatestSynced,
            SyncMode::FromInterruption => near_indexer::SyncModeEnum::FromInterruption,
            SyncMode::BlockHeight => near_indexer::SyncModeEnum::BlockHeight(args.block_height.unwrap_or(0)),
        },
        // waiting for full sync or stream messages while syncing
        await_for_node_synced: match args.await_synced {
            AwaitSynced::WaitForFullSync => near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
            AwaitSynced::StreamWhileSyncing => near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
        },
    };

    let nats_connection = nats_connect(args.to_owned());

    let system = actix::System::new();
    system.block_on(async move {
        let indexer = near_indexer::Indexer::new(indexer_config).expect("Error while creating Indexer instance");
        let events_stream = indexer.streamer();
        listen_events(args, events_stream, &nats_connection).await;
        actix::System::current().stop();
    });
    system.run().unwrap();
}

/// Listen Indexer's state events and receive `StreamerMessages` with information about finalized blocks
pub async fn listen_events(args: &RunArgs, events_stream: mpsc::Receiver<near_indexer::StreamerMessage>, nats_connection: &nats::Connection) {
    info!(
        target: "borealis_indexer",
        "Message producer loop started: listening for new messages\n"
    );
    let mut handle_messages = tokio_stream::wrappers::ReceiverStream::new(events_stream).map(|streamer_message| async {
        info!(
            target: "borealis_indexer",
            "Message producer loop executed: message received\n"
        );
        info!(
            target: "borealis_indexer",
            "block_height: #{}, block_hash: {}\n",
            &streamer_message.block.header.height,
            &streamer_message.block.header.hash
        );
        handle_message(args, streamer_message, nats_connection).await.map_err(|e| println!("Error: {}", e))
    });
    while let Some(_handled_message) = handle_messages.next().await {}
    // Graceful shutdown
    info!(target: "borealis_indexer", "Indexer will be shutted down gracefully in 10 seconds...");
    drop(handle_messages);
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
}

/// Handle Indexer's state events/messages (`StreamerMessages`) about finalized blocks
pub async fn handle_message(args: &RunArgs, streamer_message: near_indexer::StreamerMessage, nats_connection: &nats::Connection) -> anyhow::Result<()> {
    let message = message_encode(args, streamer_message.block.header.height, &streamer_message);
    message_publish(args, nats_connection, &message);
    message_dump(Some(VerbosityLevel::WithBlockHashHeight), streamer_message);
    Ok(())
}

/// Dump information from Indexer's state events/messages (`StreamerMessages`) about finalized blocks
pub fn message_dump(verbosity_level: Option<VerbosityLevel>, streamer_message: near_indexer::StreamerMessage) {
    // Data handling from `StreamerMessage` data structure. For custom filtering purposes.
    // Same as: jq '{block_height: .block.header.height, block_hash: .block.header.hash, block_header_chunk: .block.chunks[0], shard_chunk_header: .shards[0].chunk.header, transactions: .shards[0].chunk.transactions, receipts: .shards[0].chunk.receipts, receipt_execution_outcomes: .shards[0].receipt_execution_outcomes, state_changes: .state_changes}'

    if let Some(_verbosity_level) = verbosity_level {
        println!(
            "block_height: #{}, block_hash: {}\n",
            &streamer_message.block.header.height, &streamer_message.block.header.hash
        );
    };

    if let Some(VerbosityLevel::WithStreamerMessageDump) | Some(VerbosityLevel::WithStreamerMessageParse) = verbosity_level {
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

