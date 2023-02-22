use crate::aggregates::{
    Aggregate, AggregatesBucket, AggregatesQuery, AggregatesReply, AggregatesRow,
};
use crate::user_profiles::{UserProfilesQuery, UserProfilesReply};
use crate::user_tag::{Action, UserTag};
use aerospike::{
    as_bin, as_key, BatchPolicy, BatchRead, Bins, Client, ClientPolicy, Error, ErrorKind,
    Expiration, GenerationPolicy, Key, ReadPolicy, Record, ResultCode, Value, WritePolicy,
};
use anyhow::{anyhow, bail, Context};
use std::cmp::Reverse;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct DbClient {
    client: Arc<Client>,
}

impl DbClient {
    const NAMESPACE: &str = "allezon";
    const SECONDS_IN_DAY: u32 = 60 * 60 * 24;
    const PROFILE_TAGS_LIMIT: usize = 200;

    pub async fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        let client = Client::new(&ClientPolicy::default(), &addr.to_string())
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
            return Ok(vec![]);
        };
        let Value::String(tags) = bin else {
            bail!("expected the bin to be a string");
        };
        serde_json::from_str(tags).context("could not deserialize user tags")
    }

    fn parse_aggregate(record: &Record, aggregate: Aggregate) -> anyhow::Result<i64> {
        match record.bins.get(aggregate.db_name()) {
            Some(Value::Int(i)) => Ok(*i),
            Some(_) => bail!("expected bin to be an integer"),
            None => bail!("missing bin"),
        }
    }

    fn parse_batch_read(br: BatchRead) -> anyhow::Result<(i64, AggregatesRow)> {
        let time: i64 = match br.key.user_key {
            Some(Value::String(user_key)) => {
                let time = user_key
                    .split("--")
                    .next()
                    .ok_or_else(|| anyhow!("invalid user_key format"))?;
                time.parse().context("invalid user_key format")?
            }
            Some(_) => bail!("invalid user_key type"),
            None => bail!("could not get user_key"),
        };
        match br.record {
            Some(record) => Ok((
                time,
                AggregatesRow {
                    sum_price: Self::parse_aggregate(&record, Aggregate::SumPrice).with_context(
                        || format!("failed to parse {} value", Aggregate::SumPrice),
                    )?,
                    count: Self::parse_aggregate(&record, Aggregate::Count)
                        .with_context(|| format!("failed to parse {} value", Aggregate::Count))?,
                },
            )),
            None => Ok((time, (AggregatesRow::default()))),
        }
    }

    pub async fn update_user_profile(&self, user_tag: UserTag) -> anyhow::Result<()> {
        loop {
            match self.try_update_user_profile(user_tag.clone()).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(e) => return Err(e),
            }
        }
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
                let views = Self::parse_user_tags(&record, Action::View)
                    .with_context(|| format!("failed to parse {} bin", Action::View))?;

                (buys, views)
            }
            Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => {
                Default::default()
            }
            Err(e) => bail!("failed to fetch profile {:?}", e),
        };

        views.retain(|tag| {
            &tag.time >= query.time_range.from() && &tag.time < query.time_range.to()
        });
        views.truncate(query.limit as usize);
        buys.retain(|tag| {
            &tag.time >= query.time_range.from() && &tag.time < query.time_range.to()
        });
        buys.truncate(query.limit as usize);

        Ok(UserProfilesReply {
            cookie,
            views,
            buys,
        })
    }

    async fn try_update_user_profile(&self, user_tag: UserTag) -> anyhow::Result<bool> {
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
        tags.truncate(Self::PROFILE_TAGS_LIMIT);

        let as_str = serde_json::to_string(&tags).context("failed to serialize tags list")?;

        let mut policy = WritePolicy::new(generation, Expiration::Never);
        policy.generation_policy = GenerationPolicy::ExpectGenEqual;

        let bin = as_bin!(action.db_name(), as_str);

        match self.client.put(&policy, &key, &[bin]).await {
            Ok(_) => Ok(true),
            Err(Error(ErrorKind::ServerError(ResultCode::GenerationError), _)) => Ok(false),
            Err(e) => bail!("failed to update profile: {:?}", e),
        }
    }

    pub async fn get_aggregates(&self, query: AggregatesQuery) -> anyhow::Result<AggregatesReply> {
        let batch_reads = query
            .time_range
            .bucket_starts()
            .map(|time| {
                let bucket = AggregatesBucket::new(
                    time,
                    query.origin.clone(),
                    query.origin.clone(),
                    query.category_id.clone(),
                );
                let key = as_key!(Self::NAMESPACE, query.action.db_name(), bucket.to_string());
                BatchRead::new(key, Bins::All)
            })
            .collect::<Vec<_>>();

        let mut rows = self
            .client
            .batch_get(&BatchPolicy::default(), batch_reads)
            .await
            .map_err(|e| anyhow!("could not get aggregates: {:?}", e))?
            .into_iter()
            .map(Self::parse_batch_read)
            .collect::<anyhow::Result<Vec<_>>>()?;

        rows.sort_by(|x, y| x.0.cmp(&y.0));
        let rows = rows.into_iter().map(|(_, row)| row).collect();

        query.make_reply(rows)
    }

    pub async fn update_aggregate(
        &self,
        action: Action,
        bucket: &AggregatesBucket,
        count: i64,
        sum_price: i64,
    ) -> anyhow::Result<()> {
        loop {
            match self
                .try_update_aggregate(action, bucket, count, sum_price)
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_update_aggregate(
        &self,
        action: Action,
        bucket: &AggregatesBucket,
        count: i64,
        sum_price: i64,
    ) -> anyhow::Result<bool> {
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

        match self.client.put(&policy, &key, &[count, sum_price]).await {
            Ok(_) => Ok(true),
            Err(Error(ErrorKind::ServerError(ResultCode::GenerationError), _)) => Ok(false),
            Err(e) => bail!("failed to update aggregate: {:?}", e),
        }
    }
}
