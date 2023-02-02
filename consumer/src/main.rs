use anyhow::Context;
use api_server::{
    db_query::{
        time_to_int_aggregate, time_to_int_user_profile, AggregateRecord, UserProfileRecord,
        AS_BUY_PROFILES_SET, AS_KVS_BASE, AS_NAMESPACE, AS_OPERATE_BASE, AS_QUERY_PARAMS,
        AS_VIEW_PROFILES_SET,
    },
    user_tag::{Action, UserTag},
};
use async_trait::async_trait;
use event_queue::consumer::{EventProcessor, EventStream};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{net::SocketAddr, process::ExitCode};
use tokio::{
    signal,
    sync::oneshot::{self, Receiver},
};

pub mod db_ops;

use crate::db_ops::*;

struct DbNamesCombination {
    set: String,
    user_key: String,
}

struct UserTagPropertyCombinations {
    x: u8,
    action: Action,
    origin: String,
    brand_id: String,
    category_id: String,
    time: i64,
}

impl UserTagPropertyCombinations {
    const DELIMITER: &str = "___";

    fn new(tag: &UserTag) -> Self {
        Self {
            x: 0,
            action: tag.action,
            origin: tag.origin.clone(),
            brand_id: tag.product_info.brand_id.clone(),
            category_id: tag.product_info.category_id.clone(),
            time: time_to_int_aggregate(&tag.time),
        }
    }

    fn has_origin(&self) -> bool {
        self.x & 0x1 != 0
    }

    fn has_brand_id(&self) -> bool {
        self.x & 0x2 != 0
    }

    fn has_category_id(&self) -> bool {
        self.x & 0x4 != 0
    }

    fn db_names(&self) -> DbNamesCombination {
        let (mut set, mut user_key): (String, String) = match self.action {
            Action::Buy => ("buy".into(), "buy".into()),
            Action::View => ("view".into(), "view".into()),
        };
        if self.has_origin() {
            set += "-origin";
            user_key = format!("{}{}{}", user_key, Self::DELIMITER, self.origin);
        }
        if self.has_brand_id() {
            set += "-brand_id";
            user_key = format!("{}{}{}", user_key, Self::DELIMITER, self.brand_id);
        }
        if self.has_category_id() {
            set += "-category_id";
            user_key = format!("{}{}{}", user_key, Self::DELIMITER, self.category_id);
        }
        user_key = format!("{}{}{}", user_key, Self::DELIMITER, self.time);

        DbNamesCombination { set, user_key }
    }
}

impl std::iter::Iterator for UserTagPropertyCombinations {
    type Item = DbNamesCombination;

    fn next(&mut self) -> Option<Self::Item> {
        if self.x > 0x7 {
            None
        } else {
            let item = self.db_names();
            self.x += 1;
            Some(item)
        }
    }
}

struct Processor {
    db_addr: SocketAddr,
}

impl Processor {
    async fn add_to_user_profiles(tag: UserTag, db_addr: &SocketAddr) -> anyhow::Result<()> {
        let cookie = tag.cookie.clone();
        let set = match tag.action {
            Action::Buy => AS_BUY_PROFILES_SET,
            Action::View => AS_VIEW_PROFILES_SET,
        };
        let url = format!(
            "http://{}/{}/{}/{}/{}?{}",
            db_addr, AS_KVS_BASE, AS_NAMESPACE, set, cookie, AS_QUERY_PARAMS
        );
        let client = Client::new();
        let time = time_to_int_user_profile(&tag.time);
        let user_tags = vec![(time, tag.clone())];
        let record = UserProfileRecord { user_tags };
        let res = client.post(url).json(&record).send().await?;

        let url = format!(
            "http://{}/{}/{}/{}/{}?{}",
            db_addr, AS_OPERATE_BASE, AS_NAMESPACE, set, cookie, AS_QUERY_PARAMS
        );
        let res = match res.status() {
            StatusCode::CREATED => {
                let request = OperateDbRequest::list_set_order();
                client.post(url).json(&request).send().await?
            }
            StatusCode::CONFLICT => {
                let request = OperateDbRequest::update_user_profile(tag);
                client.post(url).json(&request).send().await?
            }
            _ => anyhow::bail!(
                "Could not add record {}: {:?}: {} {:?}",
                cookie,
                &record,
                res.status(),
                res.text().await?
            ),
        };
        if res.status() == StatusCode::OK {
            Ok(())
        } else {
            anyhow::bail!(
                "could not update record {}: {}, {:?}",
                cookie,
                res.status(),
                res.text().await?
            );
        }
    }

    async fn add_to_aggregates(
        tag: UserTag,
        db_names: &DbNamesCombination,
        db_addr: &SocketAddr,
    ) -> anyhow::Result<()> {
        let url = format!(
            "http://{}/{}/{}/{}/{}?{}",
            db_addr, AS_KVS_BASE, AS_NAMESPACE, db_names.set, db_names.user_key, AS_QUERY_PARAMS
        );
        let client = Client::new();
        let record = AggregateRecord {
            time: time_to_int_aggregate(&tag.time),
            count: 1,
            sum_price: tag.product_info.price as usize,
        };

        let res = client.post(url).json(&record).send().await?;
        match res.status() {
            StatusCode::CREATED => Ok(()),
            StatusCode::CONFLICT => {
                // 409: Record already exists
                let url = format!(
                    "http://{}/{}/{}/{}/{}?{}",
                    db_addr,
                    AS_OPERATE_BASE,
                    AS_NAMESPACE,
                    db_names.set,
                    db_names.user_key,
                    AS_QUERY_PARAMS
                );
                let request = OperateDbRequest::update_aggregate_record(&tag);
                let res = client.post(url).json(&request).send().await?;

                if res.status() == StatusCode::OK {
                    Ok(())
                } else {
                    anyhow::bail!(
                        "could not update record {}: {}, {:?}",
                        db_names.user_key,
                        res.status(),
                        res.text().await?,
                    );
                }
            }
            _ => anyhow::bail!(
                "Could not add record {}: {:?}: {}, {:?}",
                db_names.user_key,
                &record,
                res.status(),
                res.text().await?
            ),
        }
    }
}

#[async_trait]
impl EventProcessor for Processor {
    type Event = UserTag;

    async fn process(&self, event: UserTag) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        let tag = event.clone();
        let db_addr = self.db_addr;
        join_set.spawn(async move { Self::add_to_user_profiles(tag, &db_addr).await });
        let combinations = UserTagPropertyCombinations::new(&event);
        for db_names in combinations {
            let tag = event.clone();
            join_set.spawn(async move { Self::add_to_aggregates(tag, &db_names, &db_addr).await });
        }
        loop {
            match join_set.join_next().await {
                Some(Ok(Ok(_))) => {}
                Some(Ok(Err(e))) => {
                    anyhow::bail!("could not add user tag to database: {}", e);
                }
                Some(Err(e)) => {
                    anyhow::bail!("error while joining task: {}", e);
                }
                None => return Ok(()),
            }
        }
    }
}

#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group: String,
    kafka_topic: String,
    aerospike_rest: SocketAddr,
}

async fn run_consumer(stop: Receiver<()>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;
    let stream = EventStream::new(&args.kafka_brokers, args.kafka_group, args.kafka_topic)?;
    let processor = Processor {
        db_addr: args.aerospike_rest,
    };

    tokio::select! {
        res = stream.consume(&processor) => res,
        _ = stop => Ok (()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let (tx, rx) = oneshot::channel();
    let res = tokio::try_join!(
        async move {
            signal::ctrl_c()
                .await
                .context("failed to listen for ctrl-c")?;
            log::info!("Received a ctrl-c signal");
            tx.send(()).ok();
            Ok(())
        },
        run_consumer(rx),
    );

    match res {
        Ok(..) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("An error occurred: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
