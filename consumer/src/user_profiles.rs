use database::{client::DbClient, user_tag::UserTag};
use event_queue::consumer::EventProcessor;

pub struct UserProfilesProcessor<C> {
    db_client: C,
}

impl<C> UserProfilesProcessor<C> {
    pub fn new(db_client: C) -> Self {
        Self { db_client }
    }
}

#[async_trait::async_trait]
impl<C: DbClient + Send + Sync> EventProcessor for UserProfilesProcessor<C> {
    type Event = UserTag;

    async fn process(&self, tag: UserTag) -> anyhow::Result<()> {
        self.db_client.update_user_profile(tag).await
    }
}
