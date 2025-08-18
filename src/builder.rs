use rdkafka::{
    ClientConfig, config::RDKafkaLogLevel, consumer::StreamConsumer, error::KafkaError,
    producer::FutureProducer,
};

use crate::{
    KafkaCallbackContext,
    config::{Config, KafkaLogLevel},
};

#[derive(Debug, Clone)]
pub(crate) struct KafkaBuilder {
    inner: ClientConfig,
}

impl KafkaBuilder {
    pub(crate) fn new(config: Config) -> Self {
        let mut builder = ClientConfig::new();
        let mut brokers = String::new();

        for (i, b) in config.brokers.iter().enumerate() {
            if i != 0 {
                brokers.push(',');
            }

            brokers.push_str(b);
        }

        builder.set("bootstrap.servers", brokers);
        builder.set("group.id", &config.group_id);

        if let Some(peof) = &config.partition_eof {
            builder.set("enable.partition.eof", if *peof { "true" } else { "false" });
        }

        if let Some(session_timeout) = &config.session_timeout {
            builder.set("session.timeout.ms", session_timeout.get().to_string());
        }

        if let Some(message_timeout) = &config.message_timeout_ms {
            builder.set("message.timeout.ms", message_timeout.get().to_string());
        }

        if let Some(max_message_size) = &config.max_message_size {
            let max_message_size_kbytes = max_message_size / 1024;
            builder.set("message.max.bytes", max_message_size.to_string());
            builder.set(
                "queue.buffering.max.kbytes",
                max_message_size_kbytes.to_string(),
            );
        }

        if let Some(auto_commit) = &config.auto_commit {
            builder.set(
                "enable.auto.commit",
                if *auto_commit { "true" } else { "false" },
            );
        }

        builder.set_log_level(match config.log_level {
            KafkaLogLevel::Critical => RDKafkaLogLevel::Critical,
            KafkaLogLevel::Error => RDKafkaLogLevel::Error,
            KafkaLogLevel::Warning => RDKafkaLogLevel::Warning,
            KafkaLogLevel::Info => RDKafkaLogLevel::Info,
            KafkaLogLevel::Debug => RDKafkaLogLevel::Debug,
        });

        Self { inner: builder }
    }

    #[inline]
    pub(crate) fn build_consumer(
        &self,
    ) -> Result<StreamConsumer<KafkaCallbackContext>, KafkaError> {
        self.inner.create_with_context(KafkaCallbackContext(()))
    }

    #[inline]
    pub(crate) fn build_producer(
        &self,
    ) -> Result<FutureProducer<KafkaCallbackContext>, KafkaError> {
        self.inner.create_with_context(KafkaCallbackContext(()))
    }
}
