use anyhow::{Context, Ok};
use api_server::user_tag::UserTag;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};

pub struct TagProducer {
    producer: FutureProducer,
    topic: String,
    queue_timeout: Timeout,
}

impl TagProducer {
    pub fn new(&self, topic: String, queue_timeout: Timeout) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .create()
            .context("failed to build the Kafka producer")?;

        Ok(Self {
            producer,
            topic,
            queue_timeout,
        })
    }

    pub async fn produce(&self, tag: &UserTag) -> anyhow::Result<()> {
        let serialized = serde_json::to_vec(tag).expect("serialization to memory buffer failed");
        let record: FutureRecord<[u8], _> = FutureRecord {
            topic: &self.topic,
            partition: None,
            payload: Some(&serialized),
            key: None,
            timestamp: None,
            headers: None,
        };

        self.producer
            .send(record, self.queue_timeout)
            .await
            .map_err(|(e, _)| e)
            .context("failed to send message to Kafka")?;

        Ok(())
    }
}
