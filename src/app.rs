use crate::aggregates::{AggregatesBucket, AggregatesQuery, AggregatesReply};
use crate::db_client::DbClient;
use crate::user_profiles::{UserProfilesQuery, UserProfilesReply};
use crate::user_tag::{Action, UserTag};
use rand::Rng;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time;

#[derive(Hash, PartialEq, Eq)]
struct UpdateKey {
    bucket: AggregatesBucket,
    action: Action,
}

pub struct App {
    db_client: DbClient,
    sender: UnboundedSender<UserTag>,
}

impl App {
    pub fn new(db_client: DbClient, sender: UnboundedSender<UserTag>) -> Self {
        Self { db_client, sender }
    }

    pub async fn save_user_tag(&self, tag: UserTag) -> anyhow::Result<()> {
        self.sender.send(tag.clone())?;
        self.db_client.update_user_profile(tag).await
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

    pub fn worker(self: Arc<Self>, receiver: UnboundedReceiver<UserTag>) -> Worker {
        let millis = rand::thread_rng().gen_range(15000..30000);

        Worker {
            app: self,
            interval: Duration::from_millis(millis),
            aggregates: HashMap::new(),
            receiver,
        }
    }
}

pub struct Worker {
    app: Arc<App>,
    interval: Duration,
    aggregates: HashMap<UpdateKey, (i64, i64)>,
    receiver: UnboundedReceiver<UserTag>,
}

impl Worker {
    pub async fn run(mut self) {
        let mut ticker = time::interval(self.interval);

        loop {
            tokio::select! {
                tag = self.receiver.recv() => {
                    let Some(tag) = tag else {
                        // channel closed, time to stop
                        break;
                    };
                    self.insert_tag(tag);
                },
                _ = ticker.tick() => {
                    let aggregates = std::mem::take(&mut self.aggregates);
                    let db_client = self.app.db_client.clone();
                    tokio::spawn(Self::push_to_db(aggregates, db_client));
                }
            }
        }
    }

    fn insert_tag(&mut self, tag: UserTag) {
        for bucket in AggregatesBucket::all_buckets(&tag) {
            let key = UpdateKey {
                bucket,
                action: tag.action,
            };
            let price = tag.product_info.price as i64;
            let entry = self.aggregates.entry(key).or_default();
            entry.0 += 1;
            entry.1 += price;
        }
    }

    async fn push_to_db(
        aggregates: HashMap<UpdateKey, (i64, i64)>,
        db_client: DbClient,
    ) -> anyhow::Result<()> {
        for (key, (count, price)) in aggregates {
            let update_res = db_client
                .update_aggregate(key.action, &key.bucket, count, price)
                .await;
            if let Err(e) = update_res {
                log::error!("Failed to update aggregates: {:?}", e);
            }
        }

        Ok(())
    }
}
