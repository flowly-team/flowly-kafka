use std::marker::PhantomData;

use bytes::Bytes;
use flowly::{Decoder, Service};

use futures::{FutureExt, Stream};
use rdkafka::{
    Message as _,
    consumer::{Consumer, stream_consumer::StreamConsumer},
    error::KafkaError,
    message::Headers,
};
use tokio::sync::RwLock;

use crate::{KafkaCallbackContext, Message, builder::KafkaBuilder, config::Config, error::Error};

pub struct KafkaConsumer<M = Bytes, D: Decoder<M> = flowly::BytesDecoder> {
    builder: KafkaBuilder,
    decoder: D,
    inner: RwLock<Option<StreamConsumer<KafkaCallbackContext>>>,
    reconnect_count: u32,
    reconnect_sleep_ms: u32,
    topic: String,
    _m: PhantomData<M>,
}

impl KafkaConsumer {
    #[inline]
    pub fn new(config: Config) -> Self {
        Self::new_with_decoder(Default::default(), config)
    }
}

impl<M, D: Decoder<M>> KafkaConsumer<M, D>
where
    D::Error: std::error::Error + Send + Sync,
{
    pub fn new_with_decoder(decoder: D, config: Config) -> Self {
        Self {
            topic: config.topic.clone(),
            reconnect_count: config.reconnect_count,
            reconnect_sleep_ms: config.reconnect_sleep_ms,
            builder: KafkaBuilder::new(config),
            inner: RwLock::new(None),
            decoder,
            _m: PhantomData,
        }
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner.try_read().map(|x| x.is_some()).unwrap_or(false)
    }

    pub async fn connect(&self, topics: &[&str]) -> Result<(), Error<D::Error>> {
        log::info!(
            "trying to connect to kafka [{}]",
            topics.iter().next().unwrap()
        );

        let mut guard = self.inner.write().await;
        let _ = guard.take();

        let consumer = self.builder.build_consumer()?;
        consumer.subscribe(topics)?;
        guard.replace(consumer);

        Ok(())
    }

    async fn recv_inner(&self) -> Result<Message<M>, Error<D::Error>> {
        let guard = self.inner.read().await;
        let Some(consumer) = guard.as_ref() else {
            return Err(Error::NoConnection);
        };

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
            headers: msg.headers().map(|x| {
                x.iter()
                    .filter_map(|x| Some((x.key.to_string(), x.value?.to_vec())))
                    .collect()
            }),
        })
    }

    pub async fn recv(&self) -> Result<Message<M>, Error<D::Error>> {
        let mut reconnect_counter = if self.reconnect_count == 0 {
            u64::MAX
        } else {
            self.reconnect_count as u64
        };

        let mut error = None;

        while reconnect_counter > 0 {
            if !self.is_connected() {
                match self.connect(&[self.topic.as_str()]).await {
                    Ok(..) => (),
                    Err(err) => {
                        log::warn!("kafka connection error: {err}");
                        error.replace(err);
                        reconnect_counter -= 1;
                        tokio::time::sleep(std::time::Duration::from_millis(
                            self.reconnect_sleep_ms as _,
                        ))
                        .await;
                        continue;
                    }
                }
            }

            match self.recv_inner().await {
                Ok(msg) => {
                    return Ok(msg);
                }
                Err(Error::KafkaError(KafkaError::Transaction(e))) if e.is_fatal() => {
                    log::warn!("kafka connection error: {e}");

                    error.replace(Error::KafkaError(KafkaError::Transaction(e)));
                    reconnect_counter -= 1;
                    self.inner.write().await.take();
                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.reconnect_sleep_ms as _,
                    ))
                    .await;
                    continue;
                }

                Err(err) => return Err(err),
            }
        }

        Err(Error::TryConnectionLimit(error.map(Box::new)))
    }
}

impl<M, D, I> Service<I> for KafkaConsumer<M, D>
where
    D: Decoder<M> + Send + Sync,
    D::Error: std::error::Error + Send + Sync,
    I: AsRef<str> + Send,
    M: Send + Sync,
{
    type Out = Result<Message<M>, Error<D::Error>>;

    fn handle(&self, _input: I, _cx: &flowly::Context) -> impl Stream<Item = Self::Out> + Send {
        async move { self.recv().await }.into_stream()
    }
}
