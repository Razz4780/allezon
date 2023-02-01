use anyhow::Context;
use api_server::{
    db_query::{
        AggregateRecord, UserProfileRecord, AS_BUY_PROFILES_SET, AS_KVS_BASE, AS_NAMESPACE,
        AS_OPERATE_BASE, AS_REST_ADDR, AS_VIEW_PROFILES_SET,
    },
    user_tag::{Action, UserTag},
};
use async_trait::async_trait;
use event_queue::consumer::{EventProcessor, EventStream};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, process::ExitCode};
use tokio::{
    signal,
    sync::oneshot::{self, Receiver},
};

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
}

impl UserTagPropertyCombinations {
    const DELIMITER: &str = "|||";

    fn new(tag: &UserTag) -> Self {
        Self {
            x: 0,
            action: tag.action,
            origin: tag.origin.clone(),
            brand_id: tag.product_info.brand_id.clone(),
            category_id: tag.product_info.category_id.clone(),
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

#[allow(non_snake_case)]
#[derive(Serialize)]
struct UpdateUserProfileRecordDbRequest {
    opsList: Vec<String>,
}

impl UpdateUserProfileRecordDbRequest {
    fn new(tag: &UserTag) -> anyhow::Result<Self> {
        let tag_str = serde_json::to_string(&(-tag.time.timestamp_millis(), tag))?;
        let insert_op = format!(
            "{{
                \"type\": \"LIST_APPEND_OPERATION\",
                \"binName\": \"user_tags\",
                \"value\": {},
            }}",
            tag_str
        );
        let trim_op = format!(
            "{{
                \"type\": \"LIST_TRIM_OPERATION\",
                \"binName\": \"user_tags\",
                \"index\": 0,
                \"count\": {}
            }}",
            200
        );
        Ok(Self {
            opsList: vec![insert_op, trim_op],
        })
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
struct ListSetOrderRequest {
    opsList: Vec<String>,
}

impl ListSetOrderRequest {
    fn new() -> Self {
        let op = "
            {{
                \"type\": \"LIST_SET_ORDER_OPERATION\",
                \"binName\": \"user_tags\",
                \"listOrder\": \"ORDERED\",
            }}";
        Self {
            opsList: vec![op.into()],
        }
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
struct UpdateAggregateRecordDbRequest {
    opsList: Vec<String>,
}

impl UpdateAggregateRecordDbRequest {
    fn new(tag: &UserTag) -> Self {
        let count_add = "{{
                \"type\": \"ADD\",
                \"binName\": \"count\",
                \"incr\": 1,
            }}"
        .into();
        let sum_price_add = format!(
            "{{
                \"type\": \"ADD\",
                \"binName\": \"sum_price\",
                \"incr\": {}
            }}",
            tag.product_info.price
        );
        Self {
            opsList: vec![count_add, sum_price_add],
        }
    }
}

struct Processor;

impl Processor {
    async fn add_to_user_profiles(tag: UserTag) -> anyhow::Result<()> {
        let set = match tag.action {
            Action::Buy => AS_BUY_PROFILES_SET,
            Action::View => AS_VIEW_PROFILES_SET,
        };
        let url = format!(
            "{}/{}/{}/{}/{}",
            AS_REST_ADDR, AS_KVS_BASE, AS_NAMESPACE, set, tag.cookie
        );
        let client = Client::new();
        let user_tags = vec![(-tag.time.timestamp_millis(), tag.clone())];
        let record = UserProfileRecord { user_tags };
        let res = client.post(url).json(&record).send().await?;

        let url = format!(
            "{}/{}/{}/{}/{}",
            AS_REST_ADDR, AS_OPERATE_BASE, AS_NAMESPACE, set, tag.cookie
        );
        let res = match res.status() {
            StatusCode::OK => {
                let request = ListSetOrderRequest::new();
                client.post(url).json(&request).send().await?
            }
            StatusCode::CONFLICT => {
                let request = UpdateUserProfileRecordDbRequest::new(&tag)?;
                client.post(url).json(&request).send().await?
            }
            _ => anyhow::bail!("Could not add record {:?}: {}", &record, res.status()),
        };
        if res.status() == StatusCode::OK {
            Ok(())
        } else {
            anyhow::bail!("could not update record {}: {}", tag.cookie, res.status());
        }
    }

    async fn add_to_aggregates(tag: UserTag, db_names: &DbNamesCombination) -> anyhow::Result<()> {
        let url = format!(
            "{}/{}/{}/{}/{}",
            AS_REST_ADDR, AS_KVS_BASE, AS_NAMESPACE, db_names.set, db_names.user_key
        );
        let client = Client::new();
        let record = AggregateRecord {
            time: tag.time.timestamp() / 60,
            count: 1,
            sum_price: tag.product_info.price as usize,
        };

        let res = client.post(url).json(&record).send().await?;
        match res.status() {
            StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => {
                // 409: Record already exists
                let url = format!(
                    "{}/{}/{}/{}/{}",
                    AS_REST_ADDR, AS_OPERATE_BASE, AS_NAMESPACE, db_names.set, db_names.user_key
                );
                let request = UpdateAggregateRecordDbRequest::new(&tag);
                let res = client.post(url).json(&request).send().await?;

                if res.status() == StatusCode::OK {
                    Ok(())
                } else {
                    anyhow::bail!(
                        "could not update record {}: {}",
                        db_names.user_key,
                        res.status()
                    );
                }
            }
            _ => anyhow::bail!("Could not add record {:?}: {}", &record, res.status()),
        }
    }
}

#[async_trait]
impl EventProcessor for Processor {
    type Event = UserTag;

    async fn process(&self, event: UserTag) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        let tag = event.clone();
        join_set.spawn(async move { Self::add_to_user_profiles(tag).await });
        let combinations = UserTagPropertyCombinations::new(&event);
        for db_names in combinations {
            let tag = event.clone();
            join_set.spawn(async move { Self::add_to_aggregates(tag, &db_names).await });
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
}

async fn run_consumer(stop: Receiver<()>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;
    let stream = EventStream::new(&args.kafka_brokers, args.kafka_group, args.kafka_topic)?;

    tokio::select! {
        res = stream.consume(&Processor {}) => res,
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
