use anyhow::Context;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use tokio::sync::watch::Receiver;

#[async_trait]
pub trait EventProcessor {
    type Event: DeserializeOwned;

    async fn process(&self, event: Self::Event) -> anyhow::Result<()>;
}

pub struct EventStream {
    consumer: StreamConsumer,
    stop: Receiver<bool>,
}

impl EventStream {
    pub fn new(
        servers: &[SocketAddr],
        group: String,
        topic: &str,
        stop: Receiver<bool>,
    ) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                servers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .set("group.id", group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .create()
            .context("failed to build the Kafka consumer")?;

        consumer
            .subscribe(&[topic])
            .with_context(|| format!("failed to subscribe to the {} topic", topic))?;

        Ok(Self { consumer, stop })
    }

    pub async fn consume<P: EventProcessor>(&mut self, processor: &P) -> anyhow::Result<()> {
        let mut stream = self.consumer.stream();

        loop {
            tokio::select! {
                msg = stream.try_next() => {
                    let msg = msg?.context("failed to receive message from Kafka")?;
                    let payload = msg.payload().unwrap_or(&[]);
                    let event: P::Event = serde_json::from_slice(payload).with_context(|| {
                        format!("failed to deserialize message payload {:?}", payload)
                    })?;
                    processor
                        .process(event)
                        .await
                        .context("event consumer failed")?;

                    self.consumer
                        .store_offset_from_message(&msg)
                        .context("failed to store offset from message")?;
                }
                r = self.stop.changed() => {
                    if r.is_err() || *self.stop.borrow() {
                        break Ok(());
                    }
                }
            }
        }
    }
}
