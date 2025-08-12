# Flowly-Kafka

## Introduction

Flowly-Kafka is a Rust-based library designed to simplify interactions with Apache Kafka. This library provides a high-level abstraction for Kafka producers and consumers, making it easier to integrate Kafka into your Rust projects.

## Features

- **Async Producers and Consumers**: Utilizes asynchronous programming to handle message production and consumption efficiently.
- **Configuration Management**: Centralized management of Kafka client configurations through a structured `Config` struct.
- **Logging and Error Handling**: Provides detailed logging options and robust error handling mechanisms.

## Dependencies

The following dependencies are required to use Flowly-Kafka:

- async-stream
- bytes
- flowly
- futures
- log
- rdkafka
- serde (with derive feature)
- thiserror

## Usage

To get started with Flowly-Kafka, add the library as a dependency in your `Cargo.toml`:

```toml
[dependencies]
flowly-kafka = "0.1.0"
```

Then, you can use the library in your Rust project as follows:

```rust
use flowly_kafka::KafkaConsumer;
// Add other imports and initialization here

fn main() {
    // Initialize Flowly-Kafka with your custom configuration
    let config = /* construct your Configuration */;
    let kafka_client = KafkaConsumer::new(config);

    // Use the Kafka client to produce and consume messages
}
```

Refer to the library documentation for detailed API usage examples.

## Contributing

We welcome contributions to Flowly-Kafka. Here’s how to get started:

1. Fork the repository on GitHub.
2. Clone your forked repository locally:
   ```
   git clone https://github.com/YOUR-USERNAME/flowly-kafka.git
   cd flowly-kafka
   ```
3. Create a branch for your changes:
   ```bash
   git checkout -b add-new-feature
   ```
4. Make and commit your changes:
   ```bash
   git add .
   git commit -m "Add detailed commit message"
   ```
5. Push the changes to your forked repository:
   ```bash
   git push origin add-new-feature
   ```
6. Create a pull request from your branch to the `master` branch of the original Flowly-Kafka repository on GitHub.

## License

Flowly-Kafka is licensed under the MIT license. See [LICENSE](./LICENSE) for more details.

## Support and Feedback

For any issues or feature requests, please open an issue in the [repository's issue tracker](https://github.com/flowly-team/flowly-kafka/issues). We also appreciate feedback on how to improve our library.

---

Feel free to use Flowly-Kafka in your projects and contribute back to help improve it!
