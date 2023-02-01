use event_queue::producer::EventProducer;

use crate::user_tag::UserTag;

pub struct App {
    producer: EventProducer,
}

impl App {
    pub fn new(producer: EventProducer) -> Self {
        Self { producer }
    }

    pub async fn send_tag(&self, tag: &UserTag) -> anyhow::Result<()> {
        self.producer.produce(tag).await
    }
}
