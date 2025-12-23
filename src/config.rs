use std::{fmt, num::NonZeroU32};

use serde::Deserialize;

const DEFAULT_KAFKA_MESSAGE_SIZE: u32 = 30 * (1 << 20);

#[derive(Debug, Default, Clone, Copy, Deserialize)]
/// Enum representing different strategies for resetting the consumer offset.
pub enum AutoOffsetReset {
    /// No specific reset strategy is defined.
    None,

    /// Always start from the latest message.
    Latest,

    /// Always start from the earliest available message.
    #[default]
    Earliest,
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

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum KafkaLogLevel {
    /// Represents a critical log level.
    Critical,
    /// Represents an error log level.
    #[default]
    Error,
    /// Represents a warning log level.
    Warning,
    /// Represents an info log level.
    Info,
    /// Represents a debug log level.
    Debug,
}

impl fmt::Display for KafkaLogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaLogLevel::Debug => write!(f, "DEBUG"),
            KafkaLogLevel::Info => write!(f, "INFO"),
            KafkaLogLevel::Error => write!(f, "ERROR"),
            KafkaLogLevel::Critical => write!(f, "CRITICAL"),
            KafkaLogLevel::Warning => write!(f, "WARNING"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub brokers: Vec<String>,
    pub group_id: String,

    #[serde(default)]
    pub topic: Option<String>,

    #[serde(default)]
    pub partition_eof: Option<bool>,

    #[serde(default = "Config::default_session_timeout_ms")]
    pub session_timeout: Option<NonZeroU32>,

    #[serde(default = "Config::default_message_timeout_ms")]
    pub message_timeout_ms: Option<NonZeroU32>,

    #[serde(default = "Config::default_max_message_size")]
    pub max_message_size: Option<u32>,

    #[serde(default = "Config::default_auto_commit")]
    pub auto_commit: Option<bool>,

    #[serde(default)]
    pub auto_offset_reset: AutoOffsetReset,

    #[serde(default = "Config::default_reconnect_try_count")]
    pub reconnect_count: u32,

    #[serde(default)]
    pub log_level: KafkaLogLevel,

    #[serde(default = "Config::default_reconnect_sleep_ms")]
    pub(crate) reconnect_sleep_ms: u32,
}

#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    brokers: Vec<String>,
    group_id: Option<String>,
    topic: Option<String>,
    partition_eof: Option<bool>,
    session_timeout: Option<NonZeroU32>,
    message_timeout_ms: Option<NonZeroU32>,
    max_message_size: Option<u32>,
    auto_commit: Option<bool>,
    auto_offset_reset: AutoOffsetReset,
    reconnect_count: u32,
    reconnect_sleep_ms: u32,
    log_level: KafkaLogLevel,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigBuilder {
    pub fn new() -> Self {
        ConfigBuilder {
            brokers: Vec::new(),
            group_id: None,
            topic: None,
            partition_eof: None,
            session_timeout: Config::default_session_timeout_ms(),
            message_timeout_ms: Config::default_message_timeout_ms(),
            max_message_size: Config::default_max_message_size(),
            auto_commit: Config::default_auto_commit(),
            auto_offset_reset: AutoOffsetReset::default(),
            reconnect_count: Config::default_reconnect_try_count(),
            log_level: KafkaLogLevel::default(),
            reconnect_sleep_ms: Config::default_reconnect_sleep_ms(),
        }
    }

    /// Sets the list of broker addresses for the Kafka client.
    ///
    /// # Arguments
    ///
    /// * `brokers` - A vector of strings representing the broker addresses.
    ///
    /// # Returns
    ///
    /// The modified configuration builder instance with the specified brokers.
    pub fn brokers(mut self, brokers: Vec<String>) -> Self {
        self.brokers = brokers;
        self
    }

    /// Adds a broker to the list of existing brokers.
    ///
    /// # Arguments
    ///
    /// * `broker` - A string representing the broker to be added.
    ///
    /// # Returns
    ///
    /// This method returns the current instance with the new broker added.
    pub fn add_broker(mut self, broker: String) -> Self {
        self.brokers.push(broker);
        self
    }

    /// Sets the group ID for the current configuration.
    ///
    /// # Arguments
    ///
    /// * `group_id` - A `String` representing the group ID to be set.
    ///
    /// # Returns
    ///
    /// The current builder instance with the group ID set.
    pub fn group_id(mut self, group_id: String) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Sets the topic for the message.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string representing the topic of the message.
    ///
    /// # Returns
    ///
    /// The builder instance with the topic set.
    pub fn topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    /// Set the flag for partition EOF.
    ///
    /// # Arguments
    ///
    /// * `partition_eof` - A boolean flag
    ///
    /// # Returns
    ///
    /// The builder instance with the topic set.
    pub fn partition_eof(mut self, partition_eof: bool) -> Self {
        self.partition_eof = Some(partition_eof);
        self
    }

    /// Sets the session timeout duration.
    ///
    /// # Arguments
    ///
    /// * `session_timeout` - The timeout duration in seconds. This value must be non-zero.
    ///
    /// # Panics
    ///
    /// This method will panic if `session_timeout` is zero.
    pub fn session_timeout(mut self, session_timeout: u32) -> Self {
        self.session_timeout = Some(NonZeroU32::new(session_timeout).unwrap());
        self
    }

    /// Sets the message timeout in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `message_timeout_ms` - The timeout duration in milliseconds.
    ///
    /// # Panics
    ///
    /// This method will panic if `message_timeout_ms` is zero.
    pub fn message_timeout_ms(mut self, message_timeout_ms: u32) -> Self {
        self.message_timeout_ms = Some(NonZeroU32::new(message_timeout_ms).unwrap());
        self
    }

    /// Sets the maximum message size allowed by the builder.
    ///
    /// # Arguments
    ///
    /// * `max_message_size` - The maximum size of a message in bytes.
    ///
    /// # Returns
    ///
    /// The modified builder instance with the specified maximum message size.
    pub fn max_message_size(mut self, max_message_size: u32) -> Self {
        self.max_message_size = Some(max_message_size);
        self
    }

    /// Sets the auto-commit configuration for the Kafka consumer.
    ///
    /// When set to `true`, the consumer will automatically commit offsets after processing each record.
    ///
    /// # Arguments
    ///
    /// * `auto_commit` - A boolean indicating whether auto-commit should be enabled or not.
    pub fn auto_commit(mut self, auto_commit: bool) -> Self {
        self.auto_commit = Some(auto_commit);
        self
    }

    /// Sets the `auto.offset.reset` configuration for the Kafka consumer.
    ///
    /// This config controls the behavior of offset-autoreset for Kafka consumers when there are no initial offsets in
    /// ZK or if current offset does not exist any more on the server (e.g. because that data has been deleted).
    ///
    /// - `AutoOffsetReset::Earliest`: automatically reset to the earliest offset.
    /// - `AutoOffsetReset::Latest`: automatically reset to the latest offset.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = KafkaConfigBuilder::new()
    ///     .auto_offset_reset(AutoOffsetReset::Earliest)
    ///     .build();
    /// ```
    pub fn auto_offset_reset(mut self, auto_offset_reset: AutoOffsetReset) -> Self {
        self.auto_offset_reset = auto_offset_reset;
        self
    }

    /// Sets the maximum number of reconnection attempts for Kafka.
    ///
    /// # Arguments
    ///
    /// * `reconnect_count` - The number of times to attempt reconnecting to the Kafka broker. (default 100)
    ///
    /// # Examples
    ///
    /// ```
    /// let config = KafkaConfigBuilder::new()
    ///     .reconnect_count(5)
    ///     .build();
    /// ```
    pub fn reconnect_count(mut self, reconnect_count: u32) -> Self {
        self.reconnect_count = reconnect_count;
        self
    }

    /// Sets the time in milliseconds between reconecting to Kafka.
    ///
    /// # Arguments
    ///
    /// * `reconnect_sleep_ms` - Time in ms between reconnecting tries to the Kafka broker. (default 500ms)
    ///
    /// # Examples
    ///
    /// ```
    /// let config = KafkaConfigBuilder::new()
    ///     .reconnect_sleep_ms(500)
    ///     .build();
    /// ```
    pub fn reconnect_sleep_ms(mut self, reconnect_sleep_ms: u32) -> Self {
        self.reconnect_sleep_ms = reconnect_sleep_ms;
        self
    }

    /// Sets the log level for the Kafka configuration.
    ///
    /// # Arguments
    ///
    /// * `log_level` - The desired log level to be set.
    ///
    /// # Returns
    ///
    /// The modified `KafkaConfigBuilder` instance with the new log level.
    pub fn log_level(mut self, log_level: KafkaLogLevel) -> Self {
        self.log_level = log_level;
        self
    }

    /// Constructs a new Kafka configuration from the builder.
    ///
    /// # Returns
    ///
    /// A `Config` instance with all properties set according to the builder's state.
    pub fn build(self) -> Config {
        Config {
            brokers: self.brokers,
            group_id: self.group_id.unwrap_or_default(),
            topic: self.topic,
            partition_eof: self.partition_eof,
            session_timeout: self.session_timeout,
            message_timeout_ms: self.message_timeout_ms,
            max_message_size: self.max_message_size,
            auto_commit: self.auto_commit,
            auto_offset_reset: self.auto_offset_reset,
            reconnect_count: self.reconnect_count,
            log_level: self.log_level,
            reconnect_sleep_ms: self.reconnect_sleep_ms,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            brokers: Default::default(),
            group_id: Default::default(),
            topic: Default::default(),
            partition_eof: Default::default(),
            session_timeout: Config::default_session_timeout_ms(),
            message_timeout_ms: Config::default_message_timeout_ms(),
            max_message_size: Config::default_max_message_size(),
            auto_commit: Config::default_auto_commit(),
            auto_offset_reset: Default::default(),
            log_level: Default::default(),
            reconnect_count: Config::default_reconnect_try_count(),
            reconnect_sleep_ms: Config::default_reconnect_sleep_ms(),
        }
    }
}

impl Config {
    pub fn default_reconnect_sleep_ms() -> u32 {
        500
    }

    #[inline]
    pub fn default_session_timeout_ms() -> Option<NonZeroU32> {
        None
    }

    #[inline]
    pub fn default_message_timeout_ms() -> Option<NonZeroU32> {
        Some(NonZeroU32::new(500).unwrap())
    }

    #[inline]
    pub fn default_auto_commit() -> Option<bool> {
        None
    }

    #[inline]
    pub fn default_max_message_size() -> Option<u32> {
        Some(DEFAULT_KAFKA_MESSAGE_SIZE)
    }

    #[inline]
    pub fn default_reconnect_try_count() -> u32 {
        100
    }

    #[inline]
    pub fn default_log_level() -> KafkaLogLevel {
        KafkaLogLevel::Error
    }

    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}
