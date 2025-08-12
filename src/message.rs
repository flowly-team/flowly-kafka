use bytes::Bytes;
use chrono::{DateTime, Utc};

pub trait KafkaMessage {
    type Key: AsRef<[u8]>;
    type Value;

    fn key(&self) -> Option<Self::Key>;
    fn value(&self) -> Option<&Self::Value>;
    fn ts_ms_utc(&self) -> Option<i64>;
    fn into_value(self) -> Option<Self::Value>;
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Message<M> {
    pub key: Option<Bytes>,
    pub ts_ms_utc: Option<i64>,
    pub payload: Option<M>,
    pub partition: i32,
}

impl<M> Message<M> {
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.ts_ms_utc.and_then(DateTime::from_timestamp_millis)
    }
}

impl<M> KafkaMessage for Message<M> {
    type Key = Bytes;
    type Value = M;

    #[inline]
    fn key(&self) -> Option<Self::Key> {
        self.key.clone()
    }

    #[inline]
    fn value(&self) -> Option<&Self::Value> {
        self.payload.as_ref()
    }

    #[inline]
    fn ts_ms_utc(&self) -> Option<i64> {
        self.ts_ms_utc
    }

    #[inline]
    fn into_value(self) -> Option<Self::Value> {
        self.payload
    }
}
