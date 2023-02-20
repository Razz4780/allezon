use crate::{
    aggregates::{AggregatesBucket, AggregatesQuery, AggregatesReply},
    client::{DbClient, SimpleDbClient},
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use std::time::Duration;

#[derive(Clone)]
pub struct RetryingClient {
    client: SimpleDbClient,
    backoff: ExponentialBackoff,
}

impl RetryingClient {
    pub fn new(client: SimpleDbClient, max_elapsed_time: Duration) -> Self {
        let backoff = ExponentialBackoffBuilder::default()
            .with_max_elapsed_time(max_elapsed_time.into())
            .build();

        Self { client, backoff }
    }
}

#[async_trait::async_trait]
impl DbClient for RetryingClient {
    async fn get_user_profile(
        &self,
        cookie: String,
        query: UserProfilesQuery,
    ) -> anyhow::Result<UserProfilesReply> {
        self.client.get_user_profile(cookie, query).await
    }

    async fn update_user_profile(&self, user_tag: UserTag) -> anyhow::Result<()> {
        backoff::future::retry(self.backoff.clone(), || {
            let user_tag = user_tag.clone();
            async move {
                self.client
                    .update_user_profile(user_tag)
                    .await
                    .map_err(backoff::Error::transient)
            }
        })
        .await
        .map_err(Into::into)
    }

    async fn get_aggregates(&self, query: AggregatesQuery) -> anyhow::Result<AggregatesReply> {
        self.client.get_aggregates(query).await
    }

    async fn update_aggregate<'a>(
        &self,
        action: Action,
        bucket: AggregatesBucket<'a>,
        count: usize,
        sum_price: usize,
    ) -> anyhow::Result<()> {
        backoff::future::retry(self.backoff.clone(), || {
            let bucket = bucket.clone();
            async move {
                self.client
                    .update_aggregate(action, bucket, count, sum_price)
                    .await
                    .map_err(backoff::Error::transient)
            }
        })
        .await
        .map_err(Into::into)
    }
}
