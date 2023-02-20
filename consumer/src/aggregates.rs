use anyhow::Context;
use database::{
    aggregates::AggregatesBucket,
    client::DbClient,
    user_tag::{Action, UserTag},
};
use event_queue::consumer::{EventStream, SubStream};
use futures_util::TryStreamExt;
use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
    time::Duration,
};
use tokio::{sync::watch::Receiver, time};

#[derive(Clone, Copy)]
pub struct AggregatesFilter {
    pub origin: bool,
    pub brand_id: bool,
    pub category_id: bool,
}

impl AggregatesFilter {
    pub fn all() -> Vec<Self> {
        let mut filters = Vec::with_capacity(8);

        for i in 0..8 {
            filters.push(AggregatesFilter {
                origin: i & 1 == 0,
                brand_id: i & 2 == 0,
                category_id: i & 4 == 0,
            });
        }

        filters
    }
}

impl AggregatesFilter {
    fn make_bucket(&self, tag: &UserTag) -> AggregatesBucket {
        AggregatesBucket {
            time: tag.time,
            origin: self.origin.then(|| tag.origin.clone()),
            brand_id: self.brand_id.then(|| tag.product_info.brand_id.clone()),
            category_id: self
                .category_id
                .then(|| tag.product_info.category_id.clone()),
        }
    }
}

impl Display for AggregatesFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}--{}--{}",
            self.origin, self.brand_id, self.category_id
        )
    }
}

pub struct AggregatesProcessor<C> {
    filter: AggregatesFilter,
    db_client: C,
    stop: Receiver<bool>,
    to_mark: HashMap<SubStream, i64>,
    to_store: HashMap<(Action, AggregatesBucket), (usize, usize)>,
}

impl<C> AggregatesProcessor<C> {
    pub fn new(filter: AggregatesFilter, db_client: C, stop: Receiver<bool>) -> Self {
        Self {
            filter,
            db_client,
            stop,
            to_mark: Default::default(),
            to_store: Default::default(),
        }
    }
}

impl<C: DbClient + Send + Sync> AggregatesProcessor<C> {
    pub async fn run(mut self, stream: EventStream) -> anyhow::Result<()> {
        let events = stream.events::<UserTag>();
        tokio::pin!(events);

        let mut ticker = time::interval(Duration::from_secs(15));

        loop {
            tokio::select! {
                res = self.stop.changed() => match res {
                    Ok(_) if *self.stop.borrow() => break Ok(()),
                    Err(_) => break Ok(()),
                    Ok(_) => {},
                },
                event = events.try_next() => {
                    let event = event?.context("event stream ended unexpectedly")?;
                    let bucket = self.filter.make_bucket(&event.inner);
                    let aggregates = self.to_store.entry((event.inner.action, bucket)).or_default();
                    aggregates.0 += 1;
                    aggregates.1 += event.inner.product_info.price as usize;
                    let offset = self.to_mark.entry(event.substream).or_default();
                    *offset = event.offset;
                }
                _ = ticker.tick() => {
                    for ((action, bucket), (count, sum_price)) in self.to_store.drain() {
                        self.db_client.update_aggregate(action, bucket, count, sum_price).await.context("failed to update aggregate")?;
                    }

                    for (substream, offset) in self.to_mark.drain() {
                        stream.mark_processed(&substream, offset).context("failed to mark events as processed")?;
                    }
                }
            }
        }
    }
}
