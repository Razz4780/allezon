use database::{
    aggregates::{AggregatesBucket, AggregatesQuery, AggregatesReply},
    client::DbClient,
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};
use std::{
    collections::HashMap,
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::RwLock, time};

#[derive(Hash, PartialEq, Eq)]
struct UpdateKey {
    bucket: AggregatesBucket,
    action: Action,
}

pub struct App<C> {
    db_client: C,
    aggregates_queue: RwLock<HashMap<UpdateKey, (usize, usize)>>,
}

impl<C: DbClient> App<C> {
    pub fn new(db_client: C) -> Self {
        Self {
            db_client,
            aggregates_queue: Default::default(),
        }
    }

    pub async fn save_user_tag(&self, tag: UserTag) -> anyhow::Result<()> {
        let entries = AggregatesBucket::all_buckets(&tag)
            .map(|bucket| {
                (
                    UpdateKey {
                        bucket,
                        action: tag.action,
                    },
                    tag.product_info.price as usize,
                )
            })
            .collect::<Vec<_>>();

        self.db_client.update_user_profile(tag).await?;

        let mut guard = self.aggregates_queue.write().await;
        for (key, price) in entries {
            let entry = guard.entry(key).or_default();
            entry.0 += 1;
            entry.1 += price;
        }

        Ok(())
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

    pub fn worker(self: Arc<Self>, interval: Duration) -> Worker<C> {
        Worker {
            app: self,
            interval,
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct Worker<C> {
    app: Arc<App<C>>,
    interval: Duration,
    stop_flag: Arc<AtomicBool>,
}

impl<C: DbClient> Worker<C> {
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop_flag.clone()
    }

    pub async fn run(self) {
        let mut ticker = time::interval(self.interval);
        loop {
            ticker.tick().await;
            let mut guard = self.app.aggregates_queue.write().await;
            let work = mem::take(&mut *guard);

            if work.is_empty() && self.stop_flag.load(Ordering::Relaxed) {
                break;
            }

            for (key, (count, price)) in work {
                let update_res = self
                    .app
                    .db_client
                    .update_aggregate(key.action, key.bucket, count, price)
                    .await;
                if let Err(e) = update_res {
                    log::error!("Failed to update aggregates: {:?}", e);
                }
            }
        }
    }
}
