use crate::{
    aggregates::{Aggregate, AggregatesBucket, AggregatesRow},
    aggregates::{AggregatesQuery, AggregatesReply},
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};
use aerospike::{
    as_bin, as_key, BatchPolicy, BatchRead, Bins, Client, ClientPolicy, Error, ErrorKind,
    Expiration, GenerationPolicy, Host, Key, ReadPolicy, Record, ResultCode, Value, WritePolicy,
};
use anyhow::{anyhow, bail, Context};
use serde_json;
use std::{cmp::Reverse, net::SocketAddr, sync::Arc};

#[derive(Clone)]
pub struct DbClient {
    client: Arc<Client>,
}

impl DbClient {
    const NAMESPACE: &str = "test";
    const SECONDS_IN_DAY: u32 = 60 * 60 * 24;
    const PROFILE_TAGS_LIMIT: usize = 200;

    pub async fn new(hosts: Vec<SocketAddr>) -> anyhow::Result<Self> {
        let hosts = hosts
            .into_iter()
            .map(|addr| Host {
                name: addr.ip().to_string(),
                port: addr.port(),
            })
            .collect::<Vec<_>>();

        let client = Client::new(&ClientPolicy::default(), &hosts)
            .await
            .map_err(|e| anyhow!("could not create Aerospike client: {:?}", e))?;

        Ok(Self {
            client: client.into(),
        })
    }

    fn user_profile_key(cookie: &str) -> Key {
        as_key!(Self::NAMESPACE, "profiles", cookie)
    }

    fn parse_user_tags(record: &Record, action: Action) -> anyhow::Result<Vec<UserTag>> {
        let Some(bin) = record.bins.get(action.db_name()) else {
            bail!("bin not found");
        };

        let Value::String(tags) = bin else {
            bail!("expected the bin to be a string");
        };

        serde_json::from_str(tags).context("could not deserialize user tags")
    }

    pub async fn get_user_profile(
        &self,
        cookie: String,
        query: UserProfilesQuery,
    ) -> anyhow::Result<UserProfilesReply> {
        let key = Self::user_profile_key(&cookie);

        let request_res = self
            .client
            .get(&ReadPolicy::default(), &key, Bins::All)
            .await;
        let (mut buys, mut views) = match request_res {
            Ok(record) => {
                let buys = Self::parse_user_tags(&record, Action::Buy)
                    .with_context(|| format!("failed to parse {} bin", Action::Buy))?;
                let views = Self::parse_user_tags(&record, Action::Buy)
                    .with_context(|| format!("failed to parse {} bin", Action::View))?;

                (buys, views)
            }
            Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => {
                Default::default()
            }
            Err(e) => bail!("failed to fetch profile {:?}", e),
        };

        views.truncate(query.limit as usize);
        buys.truncate(query.limit as usize);

        Ok(UserProfilesReply {
            cookie,
            views,
            buys,
        })
    }

    pub async fn update_profile(&self, user_tag: UserTag) -> anyhow::Result<()> {
        let key = Self::user_profile_key(&user_tag.cookie);
        let action = user_tag.action;

        let request_res = self
            .client
            .get(&ReadPolicy::default(), &key, [action.db_name()])
            .await;
        let (mut tags, generation) = match request_res {
            Ok(record) => {
                let tags = Self::parse_user_tags(&record, action).context("failed to parse bin")?;
                (tags, record.generation)
            }
            Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => {
                Default::default()
            }
            Err(e) => bail!("failed to fetch profile {:?}", e),
        };

        tags.push(user_tag);
        tags.sort_unstable_by_key(|tag| Reverse(tag.time));
        tags.truncate(DbClient::PROFILE_TAGS_LIMIT);

        let as_str = serde_json::to_string(&tags).context("failed to serialize tags list")?;

        let mut policy = WritePolicy::new(generation, Expiration::Never);
        policy.generation_policy = GenerationPolicy::ExpectGenEqual;

        let bin = as_bin!(action.db_name(), as_str);

        self.client
            .put(&policy, &key, &[bin])
            .await
            .map_err(|e| anyhow!("failed to update profile: {:?}", e))?;

        Ok(())
    }

    fn parse_aggregate(record: &Record, aggregate: Aggregate) -> anyhow::Result<usize> {
        match record.bins.get(aggregate.db_name()) {
            Some(Value::Int(i)) => usize::try_from(*i).context("invalid integer value"),
            Some(_) => bail!("expected bin to be an integer"),
            None => bail!("missing bin"),
        }
    }

    pub async fn get_aggregates(&self, query: AggregatesQuery) -> anyhow::Result<AggregatesReply> {
        let batch_reads = query
            .time_range
            .bucket_starts()
            .map(|time| AggregatesBucket {
                time,
                origin: query.origin.as_deref(),
                brand_id: query.origin.as_deref(),
                category_id: query.category_id.as_deref(),
            })
            .map(|user_key| {
                let key = as_key!(
                    Self::NAMESPACE,
                    query.action.db_name(),
                    user_key.to_string()
                );
                BatchRead::new(key, Bins::All)
            })
            .collect::<Vec<_>>();

        let reads = self
            .client
            .batch_get(&BatchPolicy::default(), batch_reads)
            .await
            .map_err(|e| anyhow!("could not get aggregates: {:?}", e))?;

        let rows = reads
            .iter()
            .map(|read| read.record.as_ref())
            .map(|record| match record {
                Some(record) => Ok(AggregatesRow {
                    sum_price: Self::parse_aggregate(record, Aggregate::SumPrice).with_context(
                        || format!("failed to parse {} value", Aggregate::SumPrice),
                    )?,
                    count: Self::parse_aggregate(record, Aggregate::Count)
                        .with_context(|| format!("failed to parse {} value", Aggregate::Count))?,
                }),
                None => Ok(AggregatesRow::default()),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        query.make_reply(rows)
    }

    pub async fn update_aggregate(
        &self,
        action: Action,
        bucket: AggregatesBucket<'_>,
        count: usize,
        sum_price: usize,
    ) -> anyhow::Result<()> {
        let key = as_key!(Self::NAMESPACE, action.db_name(), bucket.to_string());

        let request_res = self
            .client
            .get(&ReadPolicy::default(), &key, Bins::All)
            .await;
        let (old_count, old_sum_price, generation) = match request_res {
            Ok(record) => {
                let count = Self::parse_aggregate(&record, Aggregate::Count)
                    .with_context(|| format!("failed to parse {} value", Aggregate::Count))?;
                let sum_price = Self::parse_aggregate(&record, Aggregate::SumPrice)
                    .with_context(|| format!("failed to parse {} value", Aggregate::SumPrice))?;
                (count, sum_price, record.generation)
            }
            Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => {
                Default::default()
            }
            Err(e) => bail!("failed to fetch profile {:?}", e),
        };

        let mut policy = WritePolicy::new(generation, Expiration::Seconds(Self::SECONDS_IN_DAY));
        policy.generation_policy = GenerationPolicy::ExpectGenEqual;

        let count = as_bin!(Aggregate::Count.db_name(), old_count + count);
        let sum_price = as_bin!(Aggregate::SumPrice.db_name(), old_sum_price + sum_price);

        self.client
            .put(&policy, &key, &[count, sum_price])
            .await
            .map_err(|e| anyhow!("failed to update aggregates: {:?}", e))?;

        Ok(())
    }
}
