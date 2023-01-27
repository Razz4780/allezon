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
    enqueue_timeout: Timeout,
}

impl EventProducer {
    pub fn new(
        servers: &[SocketAddr],
        topic: String,
        delivery_timeout_ms: u16,
        enqueue_timeout: Timeout,
    ) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                servers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .set("delivery.timeout.ms", delivery_timeout_ms.to_string())
            .create()
            .context("failed to build the Kafka producer")?;

        Ok(Self {
            producer,
            topic,
            enqueue_timeout,
        })
    }

    pub async fn produce<E: Serialize>(&self, event: &E) -> anyhow::Result<()> {
        let serialized = serde_json::to_vec(event).expect("serialization to memory buffer failed");
        let record: FutureRecord<[u8], _> = FutureRecord {
            topic: &self.topic,
            partition: None,
            payload: Some(&serialized),
            key: None,
            timestamp: None,
            headers: None,
        };

        self.producer
            .send(record, self.enqueue_timeout)
            .await
            .map_err(|(e, _)| e)
            .context("failed to send message to Kafka")?;

        Ok(())
    }
}
