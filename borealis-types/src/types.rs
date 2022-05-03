use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use serde_cbor as cbor;
use serde_cbor::Deserializer;
use serde_json;
use serde_json::Value;

pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

use std::time::SystemTime;

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
    pub fn to_cbor(&self) -> Vec<u8> {
        cbor::to_vec(&self).expect("[CBOR bytes vector: RawEvent] Message serialization error")
    }

    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self)
            .expect("[JSON bytes vector: RawEvent] Message serialization error")
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self).expect("[JSON string: RawEvent] Message serialization error")
    }

    pub fn to_json_value(&self) -> Value {
        serde_json::to_value(&self).expect("[JSON Value: RawEvent] Message serialization error")
    }
}

impl<T> RawEvent<T>
where
    T: DeserializeOwned,
{
    pub fn from_cbor(msg: &Vec<u8>) -> Option<Self> {
        if msg.len() != 0 {
            Some(
                cbor::from_slice(msg)
                    .expect("[CBOR bytes vector: RawEvent] Message deserialization error"),
            )
        } else {
            None
        }
    }

    pub fn from_json_bytes(msg: &Vec<u8>) -> Option<Self> {
        if msg.len() != 0 {
            Some(
                serde_json::from_slice(msg)
                    .expect("[JSON bytes vector: RawEvent] Message deserialization error"),
            )
        } else {
            None
        }
    }

    pub fn from_json_string(msg: &str) -> Option<Self> {
        if msg.len() != 0 {
            Some(
                serde_json::from_str(msg)
                    .expect("[JSON string: RawEvent] Message deserialization error"),
            )
        } else {
            None
        }
    }

    pub fn from_json_value(val: Value) -> Option<Self> {
        if let Some(value) = Some(val) {
            Some(
                serde_json::from_value(value)
                    .expect("[JSON Value: RawEvent] Message deserialization error"),
            )
        } else {
            None
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

// Required this way as it isn't possible to represent a struct as an array
// as expected by the CBOR RFC.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
struct EnvelopeArr(u16, u64, u32, u16, [u8; 16]);

impl From<&Envelope> for EnvelopeArr {
    fn from(envelope: &Envelope) -> Self {
        EnvelopeArr(envelope.event_type, envelope.sequential_id, envelope.timestamp_s, envelope.timestamp_ms, envelope.unique_id)
    }
}

impl Envelope {
    pub fn to_cbor(&self) -> Vec<u8> {
        let envelope_arr: EnvelopeArr = EnvelopeArr::from(self);
        cbor::to_vec(&envelope_arr).expect("[CBOR bytes vector: Envelope] Message serialization error")
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
}

/// Separately CBOR/JSON de/serialized header/envelope and body/payload data fields
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BorealisMessage<T> {
    pub version: u8,
    pub envelope: Envelope,
    //  payload can be an object, string or JSON string for further payload serialization and deserialization with the whole RawEvent message to/from CBOR or JSON format with a byte vector representration for a message transmission
    pub payload: T,
}

// Required this way as it isn't possible to represent a struct as an array
// as expected by the CBOR RFC.
pub struct BorealisMessageArr<T>(u8, EnvelopeArr, T);

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
}

impl<T> BorealisMessage<T>
where
    T: Serialize,
{
    pub fn to_cbor(&self) -> Vec<u8> {
        let envelope_ser = self.envelope.to_cbor();
        let payload_ser = cbor::to_vec(&self.payload).expect("[CBOR bytes vector: Payload] Message serialization error");

        assert_eq!(envelope_ser.len(), 30, "[CBOR vector length] CBOR vector length {} isn't 30.", envelope_ser.len());

        let mut msg_ser = Vec::with_capacity(1 + envelope_ser.len() + payload_ser.len());
        msg_ser.push(self.version);
        msg_ser.extend(envelope_ser);
        msg_ser.extend(payload_ser);

        msg_ser
    }

    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self)
            .expect("[JSON bytes vector: BorealisMessage] Message serialization error")
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self)
            .expect("[JSON string: BorealisMessage] Message serialization error")
    }

    pub fn to_json_value(&self) -> Value {
        serde_json::to_value(&self)
            .expect("[JSON Value: BorealisMessage] Message serialization error")
    }
}

impl<T> BorealisMessage<T>
where
    T: DeserializeOwned,
{
    pub fn from_cbor(msg: &[u8]) -> Option<Self> {
        if let Some((version, message)) = msg.split_first() {
            let mut chunk = Deserializer::from_slice(message).into_iter::<cbor::Value>();
            Some(Self {
                version: version.to_owned(),
                envelope: cbor::value::from_value(chunk.next().unwrap().unwrap())
                    .expect("[CBOR bytes vector: envelope] Message deserialization error"),
                payload: cbor::value::from_value(chunk.next().unwrap().unwrap())
                    .expect("[CBOR bytes vector: payload] Message deserialization error"),
            })
        } else {
            None
        }
    }

    pub fn from_json_bytes(msg: &Vec<u8>) -> Option<Self> {
        if msg.len() != 0 {
            Some(
                serde_json::from_slice(msg)
                    .expect("[JSON bytes vector: BorealisMessage] Message deserialization error"),
            )
        } else {
            None
        }
    }

    pub fn from_json_string(msg: &str) -> Option<Self> {
        if msg.len() != 0 {
            Some(
                serde_json::from_str(msg)
                    .expect("[JSON string: BorealisMessage] Message deserialization error"),
            )
        } else {
            None
        }
    }

    pub fn from_json_value(val: Value) -> Option<Self> {
        if let Some(value) = Some(val) {
            Some(
                serde_json::from_value(value)
                    .expect("[JSON Value: BorealisMessage] Message deserialization error"),
            )
        } else {
            None
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
        let cbor_out = message.to_cbor();
        let expected: Vec<u8> = vec![1, 133, 25, 11, 184, 1, 26, 98, 113, 109, 127, 25, 2, 43, 144, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 107, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100];

        assert_eq!(cbor_out, expected);
    }

    #[test]
    fn test_borealis_message_deserialization() {
        let cbor_in: Vec<u8> = vec![1, 133, 25, 11, 184, 1, 26, 98, 113, 109, 127, 25, 2, 43, 144, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 107, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100];
        let message: BorealisMessage<String> = BorealisMessage::from_cbor(&cbor_in).unwrap();

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
