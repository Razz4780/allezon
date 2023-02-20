use anyhow::{Context, Ok};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use serde::Serialize;
use std::net::SocketAddr;

pub struct EventProducer {
    producer: FutureProducer,
    topic: String,
}

impl EventProducer {
    pub fn new(servers: &[SocketAddr], topic: String) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                servers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .create()
            .context("failed to build the Kafka producer")?;

        Ok(Self { producer, topic })
    }

    pub async fn produce<E: Serialize>(&self, key: &str, event: &E) -> anyhow::Result<()> {
        let serialized = serde_json::to_vec(event).expect("serialization to memory buffer failed");
        let record: FutureRecord<_, _> = FutureRecord {
            topic: &self.topic,
            partition: None,
            payload: Some(&serialized),
            key: Some(key),
            timestamp: None,
            headers: None,
        };

        self.producer
            .send(record, Timeout::Never)
            .await
            .map_err(|(e, _)| e)
            .context("failed to send message to Kafka")?;

        Ok(())
    }
}
