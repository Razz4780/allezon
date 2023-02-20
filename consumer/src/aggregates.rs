use anyhow::Context;
use database::{aggregates::AggregatesBucket, client::DbClient, user_tag::UserTag};
use event_queue::consumer::EventProcessor;
use std::fmt::{self, Display, Formatter};

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
    fn make_bucket<'a>(&self, tag: &'a UserTag) -> AggregatesBucket<'a> {
        AggregatesBucket {
            time: tag.time,
            origin: self.origin.then_some(&tag.origin),
            brand_id: self.brand_id.then_some(&tag.product_info.brand_id),
            category_id: self.category_id.then_some(&tag.product_info.category_id),
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

pub struct AggregatesProcessor {
    filter: AggregatesFilter,
    db_client: DbClient,
}

impl AggregatesProcessor {
    pub fn new(filter: AggregatesFilter, db_client: DbClient) -> Self {
        Self { filter, db_client }
    }
}

#[async_trait::async_trait]
impl EventProcessor for AggregatesProcessor {
    type Event = UserTag;

    async fn process(&self, tag: UserTag) -> anyhow::Result<()> {
        let bucket = self.filter.make_bucket(&tag);

        self.db_client
            .update_aggregate(
                tag.action,
                bucket,
                1,
                tag.product_info
                    .price
                    .try_into()
                    .context("invalid product price")?,
            )
            .await
    }
}
