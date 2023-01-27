use anyhow::Context;
use api_server::user_tag::UserTag;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use std::net::SocketAddr;

#[async_trait]
pub trait TagProcessor {
    async fn process(&self, tag: UserTag) -> anyhow::Result<()>;
}

pub struct TagStream {
    consumer: StreamConsumer,
}

impl TagStream {
    pub fn new(servers: &[SocketAddr], group: String, topic: String) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                servers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .set("fetch.min.bytes", "50")
            .set("group.id", group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .create()
            .context("failed to build the Kafka consumer")?;

        consumer
            .subscribe(&[&topic])
            .with_context(|| format!("failed to subscribe to the {} topic", topic))?;

        Ok(Self { consumer })
    }

    pub async fn consume<P: TagProcessor>(&self, processor: &P) -> anyhow::Result<()> {
        self.consumer
            .stream()
            .map_err(anyhow::Error::from)
            .map_err(|e| e.context("failed to receive message from Kafka"))
            .try_for_each(move |msg| async move {
                let payload = msg.payload().unwrap_or(&[]);
                let tag: UserTag = serde_json::from_slice(payload).with_context(|| {
                    format!("failed to deserialize message payload {:?}", payload)
                })?;
                processor
                    .process(tag)
                    .await
                    .context("tag consumer failed")?;

                self.consumer
                    .store_offset_from_message(&msg)
                    .context("failed to store offset from message")
            })
            .await
    }
}
