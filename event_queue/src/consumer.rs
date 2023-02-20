use anyhow::Context;
use futures_util::{Stream, StreamExt, TryStreamExt};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use serde::de::DeserializeOwned;
use std::net::SocketAddr;

pub struct Event<E> {
    pub inner: E,
    pub substream: SubStream,
    pub offset: i64,
}

#[derive(Hash, PartialEq, Eq)]
pub struct SubStream {
    topic: String,
    partition: i32,
}

pub struct EventStream {
    consumer: StreamConsumer,
}

impl EventStream {
    pub fn new(servers: &[SocketAddr], group: String, topic: &str) -> anyhow::Result<Self> {
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

        Ok(Self { consumer })
    }

    pub fn events<E: DeserializeOwned>(&self) -> impl '_ + Stream<Item = anyhow::Result<Event<E>>> {
        self.consumer
            .stream()
            .map(|r| r.context("failed to receive message from kafka"))
            .and_then(|msg| async move {
                let inner: E = serde_json::from_slice(msg.payload().unwrap_or(&[]))
                    .context("failed to deserialize message")?;

                Ok(Event {
                    inner,
                    substream: SubStream {
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                    },
                    offset: msg.offset(),
                })
            })
    }

    pub fn mark_processed(&self, substream: &SubStream, offset: i64) -> anyhow::Result<()> {
        self.consumer
            .store_offset(&substream.topic, substream.partition, offset)
            .map_err(Into::into)
    }
}
