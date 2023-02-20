use database::{client::DbClient, user_tag::UserTag};
use event_queue::consumer::EventProcessor;

pub struct UserProfilesProcessor {
    db_client: DbClient,
}

impl UserProfilesProcessor {
    pub fn new(db_client: DbClient) -> Self {
        Self { db_client }
    }
}

#[async_trait::async_trait]
impl EventProcessor for UserProfilesProcessor {
    type Event = UserTag;

    async fn process(&self, tag: UserTag) -> anyhow::Result<()> {
        self.db_client.update_profile(tag).await
    }
}
