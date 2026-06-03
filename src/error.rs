use rdkafka::error::KafkaError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error<E> {
    #[error("No connection: Attempting to send or receive without an established connection")]
    NoConnection,

    #[error("Kafka error: {0}")]
    KafkaError(#[from] KafkaError),

    #[error("Message encode/decode error: {0}")]
    MessageCodecError(E),

    #[error("Kafka try connection limit: {0:?}")]
    TryConnectionLimit(Option<Box<Error<E>>>),
}
