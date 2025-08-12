pub mod builder;
pub mod config;
pub mod consumer;
pub mod error;
pub mod message;
pub mod producer;

pub use message::{KafkaMessage, Message};

struct KafkaCallbackContext(());

impl rdkafka::ClientContext for KafkaCallbackContext {
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        log::error!("Kafka global error occured: {error}, reason: {reason}. Restarting app.");
    }
}

impl rdkafka::consumer::ConsumerContext for KafkaCallbackContext {}
