use std::marker::PhantomData;

use bytes::BytesMut;
use flowly::{Encoder, Service};
use futures::{FutureExt, Stream};
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
};
use tokio::sync::RwLock;

use crate::{
    KafkaCallbackContext, KafkaMessage, builder::KafkaBuilder, config::Config, error::Error,
};

pub struct KafkaProducer<M, E> {
    encoder: E,
    builder: KafkaBuilder,
    inner: RwLock<Option<FutureProducer<KafkaCallbackContext>>>,
    topic: String,
    reconnect_count: u32,
    reconnect_sleep_ms: u32,
    _m: PhantomData<M>,
}

impl<M: Clone, E: Clone> Clone for KafkaProducer<M, E> {
    fn clone(&self) -> Self {
        Self {
            encoder: self.encoder.clone(),
            builder: self.builder.clone(),
            inner: RwLock::new(None),
            topic: self.topic.clone(),
            reconnect_count: self.reconnect_count,
            reconnect_sleep_ms: self.reconnect_sleep_ms,
            _m: self._m,
        }
    }
}

impl<M, E> KafkaProducer<M, E>
where
    M: KafkaMessage,
    E: Encoder<M::Value>,
{
    pub fn new<S: Into<String>>(encoder: E, config: Config, topic: S) -> Self {
        Self {
            encoder,
            reconnect_count: config.reconnect_count,
            reconnect_sleep_ms: config.reconnect_sleep_ms,
            builder: KafkaBuilder::new(config),
            inner: RwLock::new(None),
            topic: topic.into(),
            _m: PhantomData,
        }
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner
            .try_read()
            .map(|x| x.is_some())
            .unwrap_or_default()
    }

    pub async fn connect(&self) -> Result<(), Error<E::Error>> {
        self.inner
            .write()
            .await
            .replace(self.builder.build_producer()?);
        Ok(())
    }

    pub async fn send(&self, m: &M) -> Result<(), Error<E::Error>> {
        let guard = self.inner.read().await;
        let producer = guard.as_ref().ok_or(Error::NoConnection)?;

        let mut buffer = BytesMut::new();
        if let Some(payload) = m.value() {
            self.encoder
                .encode(payload, &mut buffer)
                .map_err(Error::MessageCodecError)?;
        }

        let record = FutureRecord::to(&self.topic);
        let key = m.key();
        let key = key.as_ref();
        let record = if let Some(key) = key {
            record.key(key.as_ref())
        } else {
            record
        };

        let record = if m.value().is_some() {
            record.payload(buffer.as_ref())
        } else {
            record
        };

        let record = if let Some(ts) = m.ts_ms_utc() {
            record.timestamp(ts)
        } else {
            record
        };

        let res = producer
            .send(record, std::time::Duration::from_secs(0))
            .await;

        match res {
            Ok(_) => Ok(()),
            Err((err, _msg)) => Err(err.into()),
        }
    }
}

impl<M, E> Service<M> for KafkaProducer<M, E>
where
    M: KafkaMessage + Send + Sync,
    M::Key: Send,
    M::Value: Send,
    E: Encoder<M::Value> + Send + Sync,
    E::Error: Send,
{
    type Out = Result<M, Error<E::Error>>;

    fn handle(&self, input: M, _cx: &flowly::Context) -> impl Stream<Item = Self::Out> + Send {
        async move {
            let mut reconnect_counter = if self.reconnect_count == 0 {
                u64::MAX
            } else {
                self.reconnect_count as u64
            };

            let mut error = None;

            while reconnect_counter > 0 {
                if !self.is_connected() {
                    match self.connect().await {
                        Ok(..) => (),
                        Err(err) => {
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

                match self.send(&input).await {
                    Ok(..) => return Ok(input),
                    Err(Error::KafkaError(KafkaError::Transaction(e))) if e.is_fatal() => {
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

            Err(error.unwrap())
        }
        .into_stream()
    }
}
