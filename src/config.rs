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
            reconnect_count: Config::default_reconnect_try_count(),
            log_level: Default::default(),
        }
    }
}

impl Config {
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
        12
    }

    #[inline]
    pub fn default_log_level() -> KafkaLogLevel {
        KafkaLogLevel::Error
    }
}
