use std::{fmt, marker::PhantomData};

use bytes::Bytes;
use flowly::{Decoder, Service};

use futures::Stream;
use rdkafka::{
    Message as _,
    consumer::{Consumer, stream_consumer::StreamConsumer},
    error::KafkaError,
};
use serde::Deserialize;

use crate::{KafkaCallbackContext, builder::KafkaBuilder, config::Config, error::Error};

#[derive(Debug, Clone, Copy, Deserialize)]
/// Enum representing different strategies for resetting the consumer offset.
pub enum AutoOffsetReset {
    /// No specific reset strategy is defined.
    None,

    /// Always start from the latest message.
    Latest,

    /// Always start from the earliest available message.
    Earliest,
}

impl Default for AutoOffsetReset {
    fn default() -> Self {
        Self::Earliest
    }
}

impl fmt::Display for AutoOffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AutoOffsetReset::None => write!(f, "none")?,
            AutoOffsetReset::Latest => write!(f, "latest")?,
            AutoOffsetReset::Earliest => write!(f, "earliest")?,
        }

        Ok(())
    }
}

pub struct Message<M> {
    pub key: Option<Bytes>,
    pub ts_ms_utc: Option<i64>,
    pub payload: Option<M>,
    pub partition: i32,
}

impl<M> Message<M> {
    pub fn timestamp(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.ts_ms_utc
            .and_then(chrono::DateTime::from_timestamp_millis)
    }
}

pub struct KafkaConsumer<M = Bytes, D: Decoder<M> = flowly::BytesDecoder> {
    builder: KafkaBuilder,
    decoder: D,
    inner: Option<StreamConsumer<KafkaCallbackContext>>,
    _m: PhantomData<M>,
    reconnect_count: u32,
}

impl KafkaConsumer {
    #[inline]
    pub fn new(config: Config) -> Self {
        Self::new_with_decoder(Default::default(), config)
    }
}

impl<M, D: Decoder<M>> KafkaConsumer<M, D> {
    pub fn new_with_decoder(decoder: D, config: Config) -> Self {
        Self {
            reconnect_count: config.reconnect_count,
            builder: KafkaBuilder::new(config),
            inner: None,
            decoder,
            _m: PhantomData,
        }
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner.is_some()
    }

    pub async fn connect(&mut self, topics: &[&str]) -> Result<(), Error<D::Error>> {
        self.inner = None;

        let consumer = self.builder.build_consumer()?;
        consumer.subscribe(topics)?;
        self.inner.replace(consumer);

        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Message<M>, Error<D::Error>> {
        let consumer = self.inner.as_mut().ok_or(Error::NoConnection)?;

        let msg = consumer.recv().await?;
        let payload = if let Some(mut msg) = msg.payload() {
            Some(
                self.decoder
                    .decode(&mut msg)
                    .map_err(Error::MessageCodecError)?,
            )
        } else {
            None
        };

        Ok(Message {
            key: msg.key().map(|x| x.to_vec().into()),
            ts_ms_utc: msg.timestamp().to_millis(),
            payload,
            partition: msg.partition(),
        })
    }
}

impl<M, D: Decoder<M>, I: for<'a> AsRef<&'a str>> Service<I> for KafkaConsumer<M, D> {
    type Out = Result<Message<M>, Error<D::Error>>;

    fn handle(&mut self, input: I, _cx: &flowly::Context) -> impl Stream<Item = Self::Out> {
        let mut reconnect_counter = self.reconnect_count + 1;
        let mut error = None;

        async_stream::stream! {
            while reconnect_counter > 0 {
                if !self.is_connected() {
                    match self.connect(&[input.as_ref()]).await {
                        Ok(..) => (),
                        Err(err) => {
                            error.replace(err);
                            reconnect_counter -= 1;
                            continue;
                        },
                    }
                }

                match self.recv().await {
                    Ok(msg) => yield Ok(msg),
                    Err(Error::KafkaError(KafkaError::Transaction(e))) if e.is_fatal() => {
                        error.replace(Error::KafkaError(KafkaError::Transaction(e)));
                        reconnect_counter -= 1;
                        self.inner = None;
                        continue;
                    }
                    Err(err) => yield Err(err),
                }
            }

            yield Err(error.unwrap().into())

        }
    }
}
