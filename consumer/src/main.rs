use aerospike::operations::lists::{ListOrderType, ListPolicy};
use aerospike::{
    as_bin, as_key, as_list, as_val, operations, Client, ClientPolicy, Error, ErrorKind,
    Expiration, GenerationPolicy, RecordExistsAction, ResultCode, WritePolicy,
};
use anyhow::{bail, Context};
use api_server::db_query::{time_to_int_aggregate, time_to_int_user_profile};
use api_server::user_tag::{Action, UserTag};
use async_trait::async_trait;
use event_queue::consumer::{EventProcessor, EventStream};
use serde::Deserialize;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use tokio::{signal, sync::oneshot};

struct DbNamesCombination {
    set: String,
    user_key: String,
}

#[derive(Clone)]
struct UserTagPropertyCombinations {
    x: u8,
    action: Action,
    origin: String,
    brand_id: String,
    category_id: String,
    time: i64,
}

impl UserTagPropertyCombinations {
    const DELIMITER: &str = "---";

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

#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group: String,
    kafka_topic: String,
    aerospike: SocketAddr,
}

#[derive(Clone)]
struct Processor {
    client: Arc<Client>,
    create_policy: Arc<WritePolicy>,
    update_policy: Arc<WritePolicy>,
}

impl Processor {
    async fn add_to_user_profiles(&self, tag: UserTag) -> anyhow::Result<()> {
        let time = time_to_int_user_profile(&tag.time);
        let tag_str = serde_json::to_string(&tag)?;
        let pair = as_list!(time, tag_str);
        let list = as_list!(pair.clone());
        let bin = as_bin!("user_tags", list);
        let key = match tag.action {
            Action::View => as_key!("test", "view-profiles", tag.cookie.clone()),
            Action::Buy => as_key!("test", "buy-profiles", tag.cookie.clone()),
        };
        match self.client.put(&self.create_policy, &key, &[bin]).await {
            Ok(_) => Ok(()),
            Err(Error(ErrorKind::ServerError(ResultCode::KeyExistsError), _))
            | Err(Error(ErrorKind::ServerError(ResultCode::GenerationError), _)) => {
                let set_order =
                    operations::lists::set_order("user_tags", ListOrderType::Ordered, &[]);
                let list_policy = ListPolicy {
                    attributes: ListOrderType::Ordered,
                    flags: operations::lists::ListWriteFlags::Default,
                };
                let append = operations::lists::append(&list_policy, "user_tags", &pair);
                let trim = operations::lists::trim("user_tags", 0, 200);
                let ops = vec![set_order, append, trim];

                match self.client.operate(&self.update_policy, &key, &ops).await {
                    Ok(_) => Ok(()),
                    Err(e) => bail!("Could not update {}: {}", tag.cookie, e),
                }
            }
            Err(e) => bail!("Could not add user tag {:?} to db: {}", tag, e),
        }
    }

    async fn add_to_aggregates(
        &self,
        price: i64,
        db_names: &DbNamesCombination,
    ) -> anyhow::Result<()> {
        let key = as_key!("test", db_names.set.as_str(), db_names.user_key.as_str());
        let count = as_bin!("count", 1);
        let sum_price = as_bin!("sum_price", price);
        let bins = [count.clone(), sum_price.clone()];

        match self.client.put(&self.create_policy, &key, &bins).await {
            Ok(_) => Ok(()),
            Err(Error(ErrorKind::ServerError(ResultCode::KeyExistsError), _))
            | Err(Error(ErrorKind::ServerError(ResultCode::GenerationError), _)) => {
                let add_count = operations::add(&count);
                let add_price = operations::add(&sum_price);
                let ops = [add_count, add_price];
                match self.client.operate(&self.update_policy, &key, &ops).await {
                    Ok(_) => Ok(()),
                    Err(e) => bail!(
                        "Could not update {}/{}: {}",
                        db_names.set,
                        db_names.user_key,
                        e
                    ),
                }
            }
            Err(e) => bail!(
                "Could not add to aggregates {}/{}: {}",
                db_names.set,
                db_names.user_key,
                e
            ),
        }
    }
}

#[async_trait]
impl EventProcessor for Processor {
    type Event = UserTag;

    async fn process(&self, event: UserTag) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        let self_copy = self.clone();
        let tag = event.clone();
        join_set.spawn(async move { self_copy.add_to_user_profiles(tag).await });
        let combinations = UserTagPropertyCombinations::new(&event);
        let price: i64 = event.product_info.price.into();
        for db_names in combinations {
            let self_copy = self.clone();
            join_set.spawn(async move { self_copy.add_to_aggregates(price, &db_names).await });
        }
        loop {
            match join_set.join_next().await {
                Some(Ok(Ok(_))) => {}
                Some(Ok(Err(e))) => {
                    bail!("could not add user tag to database: {}", e);
                }
                Some(Err(e)) => {
                    bail!("error while joining task: {}", e);
                }
                None => return Ok(()),
            }
        }
    }
}

async fn run_consumer(stop: Receiver<()>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;

    let mut create_policy = WritePolicy::new(0, Expiration::Never);
    let mut update_policy = create_policy.clone();
    create_policy.generation_policy = GenerationPolicy::ExpectGenEqual;
    create_policy.record_exists_action = RecordExistsAction::CreateOnly;
    update_policy.generation_policy = GenerationPolicy::None;
    update_policy.record_exists_action = RecordExistsAction::UpdateOnly;
    let client_policy = ClientPolicy::default();

    let client = match Client::new(&client_policy, &args.aerospike.to_string()).await {
        Ok(client) => client,
        Err(e) => bail!("Could not create client: {}", e),
    };
    let processor = Processor {
        client: client.into(),
        create_policy: create_policy.into(),
        update_policy: update_policy.into(),
    };
    let stream = EventStream::new(&args.kafka_brokers, args.kafka_group, args.kafka_topic)?;

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
