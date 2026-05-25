use std::marker::PhantomData;

use bytes::Bytes;
use flowly::{Decoder, Service};

use futures::Stream;
use rdkafka::{
    Message as _,
    consumer::{Consumer, stream_consumer::StreamConsumer},
    error::KafkaError,
    message::Headers as _,
};

use crate::{KafkaCallbackContext, Message, builder::KafkaBuilder, config::Config, error::Error};

pub struct KafkaConsumer<M = Bytes, D: Decoder<M> = flowly::BytesDecoder> {
    builder: KafkaBuilder,
    decoder: D,
    inner: Option<StreamConsumer<KafkaCallbackContext>>,
    reconnect_count: u32,
    reconnect_sleep_ms: u32,
    decode_headers: bool,
    _m: PhantomData<M>,
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
            reconnect_sleep_ms: config.reconnect_sleep_ms,
            decode_headers: config.decode_headers,
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

        let headers = if self.decode_headers
            && let Some(headers) = msg.headers()
        {
            Some(
                headers
                    .iter()
                    .filter_map(|hdr| Some((hdr.key.to_string(), hdr.value?.to_vec())))
                    .collect(),
            )
        } else {
            None
        };

        Ok(Message {
            key: msg.key().map(|x| x.to_vec().into()),
            ts_ms_utc: msg.timestamp().to_millis(),
            payload,
            partition: msg.partition(),
            headers,
        })
    }
}

impl<M, D, I> Service<I> for KafkaConsumer<M, D>
where
    D: Decoder<M> + Send,
    D::Error: std::error::Error + Send,
    I: AsRef<str> + Send,
    M: Send,
{
    type Out = Result<Message<M>, Error<D::Error>>;

    fn handle(&mut self, input: I, _cx: &flowly::Context) -> impl Stream<Item = Self::Out> + Send {
        let mut reconnect_counter = if self.reconnect_count == 0 {
            u64::MAX
        } else {
            self.reconnect_count as u64
        };

        let mut error = None;

        async_stream::stream! {
            while reconnect_counter > 0 {
                if !self.is_connected() {
                    match self.connect(&[input.as_ref()]).await {
                        Ok(..) => (),
                        Err(err) => {
                            error.replace(err);
                            reconnect_counter -= 1;
                            tokio::time::sleep(std::time::Duration::from_millis(self.reconnect_sleep_ms as _)).await;
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
                        tokio::time::sleep(std::time::Duration::from_millis(self.reconnect_sleep_ms as _)).await;
                        continue;
                    }

                    Err(err) => yield Err(err),
                }
            }

            if let Some(err) = error {
                log::error!("kafka error: {err}");

                yield Err(err);
            }
        }
    }
}
