use anyhow::Context;
use database::{client::DbClient, user_tag::UserTag};
use event_queue::consumer::EventStream;
use futures_util::TryStreamExt;
use tokio::sync::watch::Receiver;

pub struct UserProfilesProcessor<C> {
    db_client: C,
    stop: Receiver<bool>,
}

impl<C> UserProfilesProcessor<C> {
    pub fn new(db_client: C, stop: Receiver<bool>) -> Self {
        Self { db_client, stop }
    }
}

impl<C: DbClient> UserProfilesProcessor<C> {
    pub async fn run(mut self, stream: EventStream) -> anyhow::Result<()> {
        let events = stream.events::<UserTag>();
        tokio::pin!(events);

        loop {
            tokio::select! {
                res = self.stop.changed() => match res {
                    Ok(_) if *self.stop.borrow() => break Ok(()),
                    Err(_) => break Ok(()),
                    _ => {},
                },
                event = events.try_next() => {
                    let event = event?.context("event stream ended unexpectedly")?;
                    self.db_client.update_user_profile(event.inner).await.context("failed to update user profile")?;
                    stream.mark_processed(&event.substream, event.offset).context("failed to mark event as processed")?;
                }
            }
        }
    }
}
