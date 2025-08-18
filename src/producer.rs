use std::marker::PhantomData;

use bytes::BytesMut;
use flowly::{Encoder, Service};
use futures::{FutureExt, Stream};
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
};

use crate::{
    KafkaCallbackContext, KafkaMessage, builder::KafkaBuilder, config::Config, error::Error,
};

#[derive(Clone)]
pub struct KafkaProducer<M, E> {
    encoder: E,
    buffer: BytesMut,
    builder: KafkaBuilder,
    inner: Option<FutureProducer<KafkaCallbackContext>>,
    topic: String,
    _m: PhantomData<M>,
    reconnect_count: u32,
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
            builder: KafkaBuilder::new(config),
            buffer: BytesMut::new(),
            _m: PhantomData,
            inner: None,
            topic: topic.into(),
        }
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner.is_some()
    }

    pub async fn connect(&mut self) -> Result<(), Error<E::Error>> {
        self.inner = None;
        self.inner.replace(self.builder.build_producer()?);
        Ok(())
    }

    pub async fn send(&mut self, m: &M) -> Result<(), Error<E::Error>> {
        let producer = self.inner.as_mut().ok_or(Error::NoConnection)?;

        self.buffer.clear();

        if let Some(payload) = m.value() {
            self.encoder
                .encode(payload, &mut self.buffer)
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
            record.payload(self.buffer.as_ref())
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
    E: Encoder<M::Value> + Send,
    E::Error: Send,
{
    type Out = Result<(), Error<E::Error>>;

    fn handle(&mut self, input: M, _cx: &flowly::Context) -> impl Stream<Item = Self::Out> + Send {
        async move {
            let mut reconnect_counter = self.reconnect_count as i32;
            let mut error = None;

            while reconnect_counter > 0 {
                if !self.is_connected() {
                    match self.connect().await {
                        Ok(..) => (),
                        Err(err) => {
                            error.replace(err);
                            reconnect_counter -= 1;
                            continue;
                        }
                    }
                }

                match self.send(&input).await {
                    Ok(..) => return Ok(()),
                    Err(Error::KafkaError(KafkaError::Transaction(e))) if e.is_fatal() => {
                        error.replace(Error::KafkaError(KafkaError::Transaction(e)));
                        reconnect_counter -= 1;
                        self.inner = None;
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
