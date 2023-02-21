use database::{
    aggregates::{AggregatesQuery, AggregatesReply},
    client::DbClient,
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::UserTag,
};
use event_queue::producer::EventProducer;

pub struct App<C> {
    producer: EventProducer,
    db_client: C,
}

impl<C: DbClient> App<C> {
    pub fn new(producer: EventProducer, db_client: C) -> Self {
        Self {
            producer,
            db_client,
        }
    }

    pub async fn create_user_tag(&self, tag: &UserTag) -> anyhow::Result<()> {
        self.producer.produce(tag).await
    }

    pub async fn get_user_profile(
        &self,
        cookie: String,
        query: UserProfilesQuery,
    ) -> anyhow::Result<UserProfilesReply> {
        self.db_client.get_user_profile(cookie, query).await
    }

    pub async fn get_aggregates(&self, query: AggregatesQuery) -> anyhow::Result<AggregatesReply> {
        self.db_client.get_aggregates(query).await
    }
}
