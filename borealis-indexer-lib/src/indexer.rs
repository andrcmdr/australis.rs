use actix;
use borealis_producer_lib::producer::{Context, Producer};
use borealis_types::types::StreamerMessage;
use futures::StreamExt;
use nats;
use near_indexer;
use serde_cbor as cbor;
use serde_json;
use tokio::sync::mpsc;
use tokio_stream;
use tracing::info;
use tracing_subscriber::EnvFilter;

use async_trait::async_trait;

use near_indexer::near_primitives::types::Gas;

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

/// Override standard config args with library's `InitConfigArgs` type options
/// for Borealis Indexer's configurations initialization
#[derive(Debug, Clone)]
pub struct InitConfigArgs {
    /// chain/network id (localnet, devnet, testnet, betanet, mainnet)
    pub chain_id: Option<String>,
    /// Account ID for the validator key
    pub account_id: Option<String>,
    /// Specify private key generated from seed (TESTING ONLY)
    pub test_seed: Option<String>,
    /// Number of shards to initialize the chain with
    // default_value = "1"
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

/// Composite type for Borealis Indexer's configurations initialization
/// and to run NATS Producer with Borealis Indexer
#[derive(Debug, Clone)]
pub struct IndexerContext {
    pub producer: Context,
    pub indexer: InitConfigArgs,
    // default_value = "FromInterruption"
    pub sync_mode: SyncMode,
    pub block_height: Option<u64>,
    // default_value = "WaitForFullSync"
    pub await_synced: AwaitSynced,
}

impl Default for IndexerContext {
    fn default() -> Self {
        Self {
            producer: Context::default(),
            indexer: InitConfigArgs::default(),
            sync_mode: SyncMode::FromInterruption,
            block_height: None,
            await_synced: AwaitSynced::WaitForFullSync,
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
    /// Specific block height to start syncing from, Context.block_height should follow after it
    BlockHeight,
}

impl FromStr for SyncMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "latestsynced" => Ok(SyncMode::LatestSynced),
            "frominterruption" => Ok(SyncMode::FromInterruption),
            "blockheight" => Ok(SyncMode::BlockHeight),
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
        let input = s.to_lowercase();
        match input.as_str() {
            "waitforfullsync" => Ok(AwaitSynced::WaitForFullSync),
            "streamwhilesyncing" => Ok(AwaitSynced::StreamWhileSyncing),
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
        let input = s.to_lowercase();
        match input.as_str() {
            "0" | "withblockhashheight" => Ok(VerbosityLevel::WithBlockHashHeight),
            "1" | "withstreamermessagedump" => Ok(VerbosityLevel::WithStreamerMessageDump),
            "2" | "withstreamermessageparse" => Ok(VerbosityLevel::WithStreamerMessageParse),
            _ => Err("Unknown output verbosity level: `--verbose` should be `WithBlockHashHeight` (`0`), `WithStreamerMessageDump` (`1`) or `WithStreamerMessageParse` (`2`)".to_string().into()),
        }
    }
}

/// Data provider methods from IndexerContext for Indexer trait
impl Indexer for IndexerContext
where
    Self: Send + Sync + Sized + Clone + ToOwned,
{
    fn init_config_args(&self) -> InitConfigArgs {
        return self.to_owned().indexer;
    }

    fn context(&self) -> Context {
        return self.to_owned().producer;
    }

    // default_value = "FromInterruption"
    fn sync_mode(&self) -> SyncMode {
        return self.sync_mode;
    }

    fn block_height(&self) -> Option<u64> {
        return self.block_height;
    }

    // default_value = "StreamWhileSyncing"
    fn await_synced(&self) -> AwaitSynced {
        return self.await_synced;
    }
}

/// Borealis Indexer's methods to run Indexer as Producer for Borealis NATS Bus
#[async_trait]
pub trait Indexer
where
    Self: Send + Sync + Sized + Clone + ToOwned,
{
    fn init_config_args(&self) -> InitConfigArgs;

    fn context(&self) -> Context;

    // default_value = "FromInterruption"
    fn sync_mode(&self) -> SyncMode;

    fn block_height(&self) -> Option<u64>;

    // default_value = "StreamWhileSyncing"
    fn await_synced(&self) -> AwaitSynced;

    /// Initialize Indexer's configurations
    fn init(&self, home_path: Option<std::path::PathBuf>) {
        // let home_dir = home_path.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));
        let home_dir = home_path.unwrap_or(std::path::PathBuf::from("./.borealis-indexer"));

        near_indexer::indexer_init_configs(&home_dir, self.init_config_args().into())
            .expect("Error while creating Indexer's initial configuration files");
    }

    /// Run Borealis Indexer as Borealis NATS Bus Producer
    fn run(&self, home_path: Option<std::path::PathBuf>) {
        // let home_dir = home_path.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));
        let home_dir = home_path.unwrap_or(std::path::PathBuf::from("./.borealis-indexer"));

        let indexer_config = near_indexer::IndexerConfig {
            home_dir,
            // recover and continue message streaming from latest synced block (real-time), or from interruption, or from exact block height
            sync_mode: match self.sync_mode() {
                SyncMode::LatestSynced => near_indexer::SyncModeEnum::LatestSynced,
                SyncMode::FromInterruption => near_indexer::SyncModeEnum::FromInterruption,
                SyncMode::BlockHeight => {
                    near_indexer::SyncModeEnum::BlockHeight(self.block_height().unwrap_or(0))
                }
            },
            // waiting for full sync or stream messages while syncing
            await_for_node_synced: match self.await_synced() {
                AwaitSynced::WaitForFullSync => {
                    near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync
                }
                AwaitSynced::StreamWhileSyncing => {
                    near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing
                }
            },
        };

        let nats_connection = self.context().nats_connect();

        let system = actix::System::new();
        system.block_on(async move {
            let indexer = near_indexer::Indexer::new(indexer_config)
                .expect("Error while creating Indexer instance");
            let events_stream = indexer.streamer();
            self.listen_events(events_stream, &nats_connection).await;
            actix::System::current().stop();
        });
        system.run().unwrap();
    }

    /// Listen Indexer's state events and receive `StreamerMessages` with information about finalized blocks
    async fn listen_events(
        &self,
        events_stream: mpsc::Receiver<near_indexer::StreamerMessage>,
        nats_connection: &nats::Connection,
    ) {
        info!(
            target: "borealis_indexer",
            "Message producer loop started: listening for new messages\n"
        );
        let mut handle_messages = tokio_stream::wrappers::ReceiverStream::new(events_stream).map(
            |streamer_message| async {
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
                self.handle_message(streamer_message, nats_connection)
                    .await
                    .map_err(|e| println!("Error: {}", e))
            },
        );
        while let Some(_handled_message) = handle_messages.next().await {}
        // Graceful shutdown
        info!(target: "borealis_indexer", "Indexer will be shutted down gracefully in 10 seconds...");
        drop(handle_messages);
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }

    /// Handle Indexer's state events/messages (`StreamerMessages`) about finalized blocks
    async fn handle_message(
        &self,
        streamer_message: near_indexer::StreamerMessage,
        nats_connection: &nats::Connection,
    ) -> anyhow::Result<()> {
        let message = self
            .context()
            .message_encode(streamer_message.block.header.height, &streamer_message);
        self.context().message_publish(nats_connection, &message);
        // self.message_dump(Some(VerbosityLevel::WithBlockHashHeight), streamer_message);
        Ok(())
    }

    /// Dump information from Indexer's state events/messages (`StreamerMessages`) about finalized blocks
    fn message_dump(
        &self,
        verbosity_level: Option<VerbosityLevel>,
        streamer_message: StreamerMessage,
    ) {
        // Data handling from `StreamerMessage` data structure. For custom filtering purposes.
        // Same as: jq '{block_height: .block.header.height, block_hash: .block.header.hash, block_header_chunk: .block.chunks[0], shard_chunk_header: .shards[0].chunk.header, transactions: .shards[0].chunk.transactions, receipts: .shards[0].chunk.receipts, receipt_execution_outcomes: .shards[0].receipt_execution_outcomes, state_changes: .state_changes}'

        info!(
            target: "borealis_indexer",
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

                println!("StateChanges#: {}\n", shard.state_changes.len());
                shard.state_changes.iter().for_each(|state_change| {
                    println!(
                        "StateChange: {}\n",
                        serde_json::to_value(&state_change).unwrap()
                    );
                    println!("StateChange: {:?}\n", cbor::to_vec(&state_change).unwrap());
                });
            });
        };
    }
}
