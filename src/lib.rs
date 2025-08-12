pub mod builder;
pub mod config;
pub mod consumer;
pub mod error;
pub mod producer;

pub trait KafkaMessage {
    type Key: AsRef<[u8]>;
    type Value;

    fn key(&self) -> Option<Self::Key>;
    fn value(&self) -> Option<&Self::Value>;
    fn ts_ms_utc(&self) -> Option<i64>;
    fn into_value(self) -> Option<Self::Value>;
}

struct KafkaCallbackContext(());

impl rdkafka::ClientContext for KafkaCallbackContext {
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        log::error!("Kafka global error occured: {error}, reason: {reason}. Restarting app.");
    }
}

impl rdkafka::consumer::ConsumerContext for KafkaCallbackContext {}
