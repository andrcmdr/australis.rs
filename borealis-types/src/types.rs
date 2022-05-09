use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use serde_cbor as cbor;
use serde_cbor::Deserializer;
use serde_json;
use serde_json::Value;

use lzzzz::lz4f::{BlockSize, PreferencesBuilder, CLEVEL_MAX, compress_to_vec, decompress_to_vec};
use zstd::bulk::{compress_to_buffer, decompress_to_buffer};

pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

use std::time::SystemTime;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub const VERSION: u8 = 1;
pub const EVENT_TYPE: u16 = 4096;
pub const BOREALIS_EPOCH: u64 = 1231006505; // Conform to Bitcoin genesis, 2009-01-03T18:15:05Z

/// According to:
/// https://github.com/aurora-is-near/borealis.go/blob/a17d266a7a4e0918db743db474332e8474e90f35/raw_event.go#L18-L26
/// https://github.com/aurora-is-near/borealis.go/blob/a17d266a7a4e0918db743db474332e8474e90f35/events.go#L14-L19
/// https://github.com/aurora-is-near/borealis-events#message-format
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RawEvent<T> {
    pub version: u8,
    pub event_type: u16,
    pub sequential_id: u64,
    pub timestamp_s: u32,
    pub timestamp_ms: u16,
    pub unique_id: [u8; 16],
    //  payload can be an object, string or JSON string for further payload serialization and deserialization with the whole RawEvent message to/from CBOR or JSON format with a byte vector representration for a message transmission
    pub payload: T,
}

impl<T> RawEvent<T> {
    pub fn new(sequential_id: u64, payload: T) -> Self {
        let version = VERSION;
        let event_type = EVENT_TYPE;
        let now = SystemTime::UNIX_EPOCH.elapsed().unwrap();
        let timestamp_s = (now.as_secs() - BOREALIS_EPOCH) as u32;
        let timestamp_ms = (now.as_millis() % 1000) as u16;
        let unique_id = rand::random();

        Self {
            version,
            event_type,
            sequential_id,
            timestamp_s,
            timestamp_ms,
            unique_id,
            payload,
        }
    }
}

impl<T> RawEvent<T>
where
    T: Serialize,
{
    pub fn to_cbor(&self) -> Result<Vec<u8>, Error> {
        let result = cbor::to_vec(&self);
        match result {
            Ok(cbor_bytes) => Ok(cbor_bytes),
            Err(error) => {
                Err(format!("[CBOR bytes vector: RawEvent] Message serialization error: {:?}", error).into())
            },
        }
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        let result = serde_json::to_vec(&self);
        match result {
            Ok(json_bytes) => Ok(json_bytes),
            Err(error) => {
                Err(format!("[JSON bytes vector: RawEvent] Message serialization error: {:?}", error).into())
            },
        }
    }

    pub fn to_json_string(&self) -> Result<String,Error> {
        let result = serde_json::to_string(&self);
        match result {
            Ok(json_string) => Ok(json_string),
            Err(error) => {
                Err(format!("[JSON string: RawEvent] Message serialization error: {:?}", error).into())
            },
        }
    }

    pub fn to_json_value(&self) -> Result<Value, Error> {
        let result = serde_json::to_value(&self);
        match result {
            Ok(json_value) => Ok(json_value),
            Err(error) => {
                Err(format!("[JSON Value: RawEvent] Message serialization error: {:?}", error).into())
            },
        }
    }
}

impl<T> RawEvent<T>
where
    T: DeserializeOwned,
{
    pub fn from_cbor(msg: &[u8]) -> Result<Option<Self>, Error> {
        if !msg.is_empty() {
            let result = cbor::from_slice::<Self>(msg);
            match result {
                Ok(message) => Ok(Some(message)),
                Err(error) => {
                    Err(format!("[CBOR bytes vector: RawEvent] Message deserialization error: {:?}", error).into())
                },
            }
        } else {
            Ok(None)
        }
    }

    pub fn from_json_bytes(msg: &[u8]) -> Result<Option<Self>, Error> {
        if !msg.is_empty() {
            let result = serde_json::from_slice::<Self>(msg);
            match result {
                Ok(message) => Ok(Some(message)),
                Err(error) => {
                    Err(format!("[JSON bytes vector: RawEvent] Message deserialization error: {:?}", error).into())
                },
            }
        } else {
            Ok(None)
        }
    }

    pub fn from_json_string(msg: &str) -> Result<Option<Self>, Error> {
        if !msg.is_empty() {
            let result = serde_json::from_str::<Self>(msg);
            match result {
                Ok(message) => Ok(Some(message)),
                Err(error) => {
                    Err(format!("[JSON string: RawEvent] Message deserialization error: {:?}", error).into())
                },
            }
        } else {
            Ok(None)
        }
    }

    pub fn from_json_value(val: Value) -> Result<Self, Error> {
        let result = serde_json::from_value::<Self>(val);
        match result {
            Ok(message) => Ok(message),
            Err(error) => {
                Err(format!("[JSON Value: RawEvent] Message deserialization error: {:?}", error).into())
            },
        }
    }
}

/// Borealis Message header
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub event_type: u16,
    pub sequential_id: u64,
    pub timestamp_s: u32,
    pub timestamp_ms: u16,
    pub unique_id: [u8; 16],
}

/// Borealis Message header, represented as tuple structure, with anonymous unnamed fields,
/// 'cause header/envelope in a CBOR chunk of data should be represented as an array, using serde_cbor.
/// Required for following CBOR RFC and compatibility with CBOR decoders existed for other languages.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
struct EnvelopeArr(u16, u64, u32, u16, [u8; 16]);

impl From<&Envelope> for EnvelopeArr {
    fn from(envelope: &Envelope) -> Self {
        EnvelopeArr(
            envelope.event_type,
            envelope.sequential_id,
            envelope.timestamp_s,
            envelope.timestamp_ms,
            envelope.unique_id
        )
    }
}

impl Envelope {
    pub fn new(sequential_id: u64) -> Self {
        let event_type = EVENT_TYPE;
        let now = SystemTime::UNIX_EPOCH.elapsed().unwrap();
        let timestamp_s = (now.as_secs() - BOREALIS_EPOCH) as u32;
        let timestamp_ms = (now.as_millis() % 1000) as u16;
        let unique_id = rand::random();

        Self {
            event_type,
            sequential_id,
            timestamp_s,
            timestamp_ms,
            unique_id,
        }
    }

    pub fn to_cbor_arr(&self) -> Result<Vec<u8>, Error> {
        let envelope_arr: EnvelopeArr = EnvelopeArr::from(self);
        let result = cbor::to_vec(&envelope_arr);
        match result {
            Ok(cbor_bytes) => Ok(cbor_bytes),
            Err(error) => {
                Err(format!("[CBOR bytes vector: Envelope] Message header serialization error: {:?}", error).into())
            },
        }
    }
}

/// Separately CBOR/JSON de/serialized header/envelope and body/payload data fields
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BorealisMessage<T> {
    pub version: u8,
    pub envelope: Envelope,
    //  payload can be an object, string or JSON string for further payload serialization and deserialization with the whole RawEvent message to/from CBOR or JSON format with a byte vector representration for a message transmission
    pub payload: T,
}

impl<T> BorealisMessage<T> {
    pub fn new(sequential_id: u64, payload: T) -> Self {
        let version = VERSION;
        let envelope = Envelope::new(sequential_id);

        Self {
            version,
            envelope,
            payload,
        }
    }

    pub fn payload_compress_lz4(payload: &[u8]) -> Result<(Vec<u8>, usize), Error> {
        let prefs = PreferencesBuilder::new()
            .block_size(BlockSize::Max1MB)
            .compression_level(CLEVEL_MAX)
            .build();

        let mut compressed_payload = Vec::with_capacity(payload.len());

        let result = compress_to_vec(payload, &mut compressed_payload, &prefs);

        match result {
            Ok(output_len) => Ok((compressed_payload, output_len)),
            Err(error) => {
                Err(format!("[LZ4 compressed bytes vector: Payload] Message body compression error: {:?}", error).into())
            },
        }
    }

    pub fn payload_decompress_lz4(compressed_payload: &[u8]) -> Result<(Vec<u8>, usize), Error> {
        let mut decompressed_payload = Vec::new();

        let result = decompress_to_vec(compressed_payload, &mut decompressed_payload);

        match result {
            Ok(output_len) => Ok((decompressed_payload, output_len)),
            Err(error) => {
                Err(format!("[LZ4 compressed bytes vector: Payload] Message body decompression error: {:?}", error).into())
            },
        }
    }

    pub fn payload_compress_zstd(payload: &[u8]) -> Result<(Vec<u8>, usize), Error> {
        let mut compressed_payload = Vec::with_capacity(payload.len());

        let result = compress_to_buffer(payload, &mut compressed_payload, 19);

        match result {
            Ok(output_len) => Ok((compressed_payload, output_len)),
            Err(error) => {
                Err(format!("[Zstd compressed bytes vector: Payload] Message body compression error: {:?}", error).into())
            },
        }
    }

    pub fn payload_decompress_zstd(compressed_payload: &[u8]) -> Result<(Vec<u8>, usize), Error> {
        let mut decompressed_payload = Vec::new();

        let result = decompress_to_buffer(compressed_payload, &mut decompressed_payload);

        match result {
            Ok(output_len) => Ok((decompressed_payload, output_len)),
            Err(error) => {
                Err(format!("[Zstd compressed bytes vector: Payload] Message body decompression error: {:?}", error).into())
            },
        }
    }
}

impl<T> BorealisMessage<T>
where
    T: Serialize,
{
    pub fn to_cbor(&self) -> Result<Vec<u8>, Error> {
        let envelope_ser = match self.envelope.to_cbor_arr() {
            Ok(envelope_bytes) => envelope_bytes,
            Err(error) => {
                return Err(format!("[CBOR bytes vector: Envelope] Message header serialization error: {:?}", error).into())
            },
        };

        let payload_ser = match cbor::to_vec(&self.payload) {
            Ok(payload_bytes) => payload_bytes,
            Err(error) => {
                return Err(format!("[CBOR bytes vector: Payload] Message body serialization error: {:?}", error).into())
            },
        };

        let mut message_ser = Vec::with_capacity(1 + envelope_ser.len() + payload_ser.len());

        message_ser.push(self.version);
        message_ser.extend(envelope_ser);
        message_ser.extend(payload_ser);

        Ok(message_ser)
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        let result = serde_json::to_vec(&self);
        match result {
            Ok(json_bytes) => Ok(json_bytes),
            Err(error) => {
                Err(format!("[JSON bytes vector: BorealisMessage] Message serialization error: {:?}", error).into())
            },
        }
    }

    pub fn to_json_string(&self) -> Result<String, Error> {
        let result = serde_json::to_string(&self);
        match result {
            Ok(json_string) => Ok(json_string),
            Err(error) => {
                Err(format!("[JSON string: BorealisMessage] Message serialization error: {:?}", error).into())
            },
        }
    }

    pub fn to_json_value(&self) -> Result<Value, Error> {
        let result = serde_json::to_value(&self);
        match result {
            Ok(json_value) => Ok(json_value),
            Err(error) => {
                Err(format!("[JSON Value: BorealisMessage] Message serialization error: {:?}", error).into())
            },
        }
    }
}

impl<T> BorealisMessage<T>
where
    T: DeserializeOwned,
{
    pub fn from_cbor(msg: &[u8]) -> Result<Option<Self>, Error> {
        if !msg.is_empty() {
            if let Some((version, message)) = msg.split_first() {
                let mut chunk = Deserializer::from_slice(message).into_iter::<cbor::Value>();

                let envelope_value = match chunk.next() { 
                    Some(Ok(envelope_value)) => envelope_value,
                    Some(Err(error)) => {
                        return Err(format!("[CBOR bytes vector: Envelope Value] Message header deserialization error: {:?}", error).into())
                    },
                    None => {
                        return Err(format!("[CBOR bytes vector: Envelope Value] Message header deserialization error: Envelope value is empty").into())
                    },
                };

                let payload_value = match chunk.next() {
                    Some(Ok(payload_value)) => payload_value,
                    Some(Err(error)) => {
                        return Err(format!("[CBOR bytes vector: Payload Value] Message body deserialization error: {:?}", error).into())
                    },
                    None => {
                        return Err(format!("[CBOR bytes vector: Payload Value] Message body deserialization error: Payload value is empty").into())
                    },
                };

                let envelope = match cbor::value::from_value::<Envelope>(envelope_value) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        return Err(format!("[CBOR bytes vector: Envelope] Message header deserialization error: {:?}", error).into())
                    },
                };

                let payload = match cbor::value::from_value::<T>(payload_value) {
                    Ok(payload) => payload,
                    Err(error) => {
                        return Err(format!("[CBOR bytes vector: Payload] Message body deserialization error: {:?}", error).into())
                    },
                };

                Ok(Some(Self {
                    version: version.to_owned(),
                    envelope,
                    payload,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn from_json_bytes(msg: &[u8]) -> Result<Option<Self>, Error> {
        if !msg.is_empty() {
            let result = serde_json::from_slice::<Self>(msg);
            match result {
                Ok(message) => Ok(Some(message)),
                Err(error) => {
                    Err(format!("[JSON bytes vector: BorealisMessage] Message deserialization error: {:?}", error).into())
                },
            }
        } else {
            Ok(None)
        }
    }

    pub fn from_json_string(msg: &str) -> Result<Option<Self>, Error> {
        if !msg.is_empty() {
            let result = serde_json::from_str::<Self>(msg);
            match result {
                Ok(message) => Ok(Some(message)),
                Err(error) => {
                    Err(format!("[JSON string: BorealisMessage] Message deserialization error: {:?}", error).into())
                },
            }
        } else {
            Ok(None)
        }
    }

    pub fn from_json_value(val: Value) -> Result<Self, Error> {
        let result  = serde_json::from_value::<Self>(val);
        match result {
            Ok(message) => Ok(message),
            Err(error) => {
                Err(format!("[JSON Value: BorealisMessage] Message deserialization error: {:?}", error).into())
            },
        }
    }
}

/// Resulting struct represents block with chunks
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamerMessage {
    pub block: views::BlockView,
    pub shards: Vec<IndexerShard>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerShard {
    pub shard_id: types::ShardId,
    pub chunk: Option<IndexerChunkView>,
    pub receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceipt>,
    pub state_changes: views::StateChangesView,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerChunkView {
    pub author: types::AccountId,
    pub header: views::ChunkHeaderView,
    pub transactions: Vec<IndexerTransactionWithOutcome>,
    pub receipts: Vec<views::ReceiptView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerTransactionWithOutcome {
    pub transaction: views::SignedTransactionView,
    pub outcome: IndexerExecutionOutcomeWithOptionalReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerExecutionOutcomeWithOptionalReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: Option<views::ReceiptView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerExecutionOutcomeWithReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: views::ReceiptView,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_borealis_message_serialization() {
        let message = BorealisMessage {
            version: 1,
            envelope: Envelope {
                event_type: 3000,
                sequential_id: 1,
                timestamp_s: 1651600767,
                timestamp_ms: 555,
                unique_id: [1; 16],
            },
            payload: "hello world".to_string()
        };

        let cbor_out = message.to_cbor().unwrap();
        let expected: Vec<u8> = vec![1, 133, 25, 11, 184, 1, 26, 98, 113, 109, 127, 25, 2, 43, 144, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 107, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100];

        assert_eq!(cbor_out, expected);
    }

    #[test]
    fn test_borealis_message_deserialization() {
        let cbor_in: Vec<u8> = vec![1, 133, 25, 11, 184, 1, 26, 98, 113, 109, 127, 25, 2, 43, 144, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 107, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100];
        let message: BorealisMessage<String> = BorealisMessage::from_cbor(&cbor_in).unwrap().unwrap();

        let expected = BorealisMessage {
            version: 1,
            envelope: Envelope {
                event_type: 3000,
                sequential_id: 1,
                timestamp_s: 1651600767,
                timestamp_ms: 555,
                unique_id: [1; 16],
            },
            payload: "hello world".to_string()
        };

        assert_eq!(message, expected);
    }
}
