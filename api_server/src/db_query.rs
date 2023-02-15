use crate::{
    aggregates::{Aggregate, AggregatesRow},
    aggregates::{AggregatesQuery, AggregatesReply},
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};
use aerospike::{
    as_key, BatchPolicy, BatchRead, Bins, Client, ClientPolicy, Error, ErrorKind, ReadPolicy,
    Record, ResultCode, Value,
};
use anyhow::{anyhow, bail, ensure};
use chrono::{DateTime, Utc};
use serde_json;
use std::net::SocketAddr;

pub fn time_to_int_user_profile(time: &DateTime<Utc>) -> i64 {
    -time.timestamp_millis()
}

pub fn time_to_int_aggregate(time: &DateTime<Utc>) -> i64 {
    time.timestamp() / 60
}

#[derive(Clone, Copy)]
pub struct AggregatesCombination {
    pub x: u8,
}

impl AggregatesCombination {
    fn has_origin(&self) -> bool {
        self.x & 0x1 != 0
    }

    fn has_brand_id(&self) -> bool {
        self.x & 0x2 != 0
    }

    fn has_category_id(&self) -> bool {
        self.x & 0x4 != 0
    }

    pub fn from_aggregates_query(query: &AggregatesQuery) -> Self {
        let mut x = 0;
        if query.origin.is_some() {
            x |= 0x1;
        }
        if query.brand_id.is_some() {
            x |= 0x2;
        }
        if query.category_id.is_some() {
            x |= 0x4;
        }
        Self { x }
    }

    pub fn set(&self, action: Action) -> String {
        let mut ret: String = match action {
            Action::Buy => "buy",
            Action::View => "view",
        }
        .into();
        if self.has_origin() {
            ret += "-origin";
        }
        if self.has_brand_id() {
            ret += "-brand_id";
        }
        if self.has_category_id() {
            ret += "-category_id";
        }
        ret
    }

    fn user_key(
        &self,
        action: Action,
        origin: &str,
        brand_id: &str,
        category_id: &str,
        time: i64,
    ) -> String {
        let mut ret: String = match action {
            Action::Buy => "buy",
            Action::View => "view",
        }
        .into();
        if self.has_origin() {
            ret = format!("{}---{}", ret, origin);
        }
        if self.has_brand_id() {
            ret = format!("{}---{}", ret, brand_id);
        }
        if self.has_category_id() {
            ret = format!("{}---{}", ret, category_id);
        }
        format!("{}---{}", ret, time)
    }

    pub fn user_key_from_tag(&self, tag: &UserTag) -> String {
        let time = time_to_int_aggregate(&tag.time);
        self.user_key(
            tag.action,
            tag.origin.as_str(),
            tag.product_info.brand_id.as_str(),
            tag.product_info.category_id.as_str(),
            time,
        )
    }

    pub fn user_key_iter(query: &AggregatesQuery) -> impl '_ + Iterator<Item = String> {
        let action = query.action;
        let origin = query.origin.clone().unwrap_or_default();
        let brand_id = query.brand_id.clone().unwrap_or_default();
        let category_id = query.category_id.clone().unwrap_or_default();
        let combination = Self::from_aggregates_query(query);
        query.time_range.bucket_starts().map(move |t| {
            let time = time_to_int_aggregate(&t);
            combination.user_key(
                action,
                origin.as_str(),
                brand_id.as_str(),
                category_id.as_str(),
                time,
            )
        })
    }
}

pub struct DbClient {
    user_profiles_client: Client,
    aggregates_clients: Vec<Client>,
    read_policy: ReadPolicy,
    batch_policy: BatchPolicy,
}

impl DbClient {
    pub async fn new(
        user_profiles_addr: SocketAddr,
        aggregates_addr: Vec<SocketAddr>,
    ) -> anyhow::Result<Self> {
        ensure!(aggregates_addr.len() == 4, "invalid env args");

        let read_policy = ReadPolicy::default();
        let batch_policy = BatchPolicy::default();
        let user_profiles_client =
            Client::new(&ClientPolicy::default(), &user_profiles_addr.to_string()).await;
        let user_profiles_client = match user_profiles_client {
            Ok(client) => client,
            Err(e) => bail!("Could not create user profiles client: {}", e),
        };
        let mut aggregates_clients = vec![];
        for addr in aggregates_addr.into_iter() {
            let client = match Client::new(&ClientPolicy::default(), &addr.to_string()).await {
                Ok(client) => client,
                Err(e) => bail!("Could not create aggregates client: {}", e),
            };
            aggregates_clients.push(client);
        }

        Ok(Self {
            user_profiles_client,
            aggregates_clients,
            read_policy,
            batch_policy,
        })
    }

    fn record_to_user_tags(record: Record, query: &UserProfilesQuery) -> Option<Vec<UserTag>> {
        match record.bins.get("user_tags") {
            Some(Value::List(tags)) => {
                let l = time_to_int_user_profile(query.time_range.from());
                let r = time_to_int_user_profile(query.time_range.to());
                let tags = tags
                    .iter()
                    .filter_map(|v| match v {
                        Value::List(pair) => match &pair[..] {
                            [Value::Int(time), Value::String(tag_str)] => Some((time, tag_str)),
                            _ => None,
                        },
                        _ => None,
                    })
                    .skip_while(|&(t, _)| *t <= r)
                    .take_while(|&(t, _)| *t <= l)
                    .take(query.limit as usize)
                    .filter_map(|(_, tag)| serde_json::from_str(tag).ok())
                    .collect();
                Some(tags)
            }
            _ => None,
        }
    }

    async fn get_user_tags(
        &self,
        cookie: &str,
        query: &UserProfilesQuery,
        action: Action,
    ) -> anyhow::Result<Vec<UserTag>> {
        let set = match action {
            Action::View => "view-profiles",
            Action::Buy => "buy-profiles",
        };
        let key = as_key!("test", set, cookie);
        match self
            .user_profiles_client
            .get(&self.read_policy, &key, ["user_tags"])
            .await
        {
            Ok(record) => Self::record_to_user_tags(record, query)
                .ok_or_else(|| anyhow!("Could not get {} of {}: invalid record", set, cookie)),
            Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => Ok(vec![]),
            Err(e) => bail!("Could not get {} of {}: {}", set, cookie, e),
        }
    }

    pub async fn get_user_profile(
        &self,
        cookie: String,
        query: &UserProfilesQuery,
    ) -> anyhow::Result<UserProfilesReply> {
        let (views, buys) = tokio::try_join!(
            self.get_user_tags(cookie.as_str(), query, Action::View),
            self.get_user_tags(cookie.as_str(), query, Action::Buy),
        )?;
        Ok(UserProfilesReply {
            cookie,
            views,
            buys,
        })
    }

    fn value_to_int(value: Option<&Value>) -> Option<i64> {
        match value {
            Some(&Value::Int(i)) => Some(i),
            _ => None,
        }
    }

    fn get_record_bins(
        record: BatchRead,
        has_count: bool,
        has_sum_price: bool,
    ) -> Option<(i64, AggregatesRow)> {
        let user_key = record.key.user_key?;
        let time: i64 = match user_key {
            Value::String(user_key) => user_key.split("---").last()?.parse().ok()?,
            _ => return None,
        };
        let record = record.record?;
        let count = Self::value_to_int(record.bins.get("count"));
        let sum_price = Self::value_to_int(record.bins.get("sum_price"));
        let (count, sum_price) = match (count, sum_price) {
            (Some(c), Some(sp)) if has_count && has_sum_price => {
                (Some(c as usize), Some(sp as usize))
            }
            (Some(c), _) if has_count && !has_sum_price => (Some(c as usize), None),
            (_, Some(sp)) if !has_count && has_sum_price => (None, Some(sp as usize)),
            _ => return None,
        };
        let row = AggregatesRow { sum_price, count };
        Some((time, row))
    }

    fn records_to_aggregates_reply(
        records: Vec<BatchRead>,
        query: AggregatesQuery,
    ) -> anyhow::Result<AggregatesReply> {
        let has_count = query.aggregates().contains(&Aggregate::Count);
        let has_sum_price = query.aggregates().contains(&Aggregate::SumPrice);

        let mut db_rows: Vec<(i64, AggregatesRow)> = records
            .into_iter()
            .filter_map(|b| Self::get_record_bins(b, has_count, has_sum_price))
            .collect();
        db_rows.sort_by(|a, b| a.0.cmp(&b.0));
        let mut reply_rows = vec![];
        let mut db_rows_idx = 0;
        for start_time in query.time_range.bucket_starts() {
            let start_time = time_to_int_aggregate(&start_time);
            let row = if db_rows_idx < db_rows.len() && db_rows[db_rows_idx].0 == start_time {
                db_rows_idx += 1;
                db_rows[db_rows_idx - 1].1.clone()
            } else {
                AggregatesRow {
                    sum_price: has_sum_price.then_some(0),
                    count: has_count.then_some(0),
                }
            };
            reply_rows.push(row);
        }
        ensure!(
            db_rows_idx == db_rows.len(),
            "Error while creating aggregates reply. Query {:?}, db_rows {:?}, reply_rows {:?}",
            query,
            db_rows,
            reply_rows
        );
        query.make_reply(reply_rows)
    }

    pub async fn get_aggregate(&self, query: AggregatesQuery) -> anyhow::Result<AggregatesReply> {
        let combination = AggregatesCombination::from_aggregates_query(&query);
        let client_idx = if combination.x & !0x3 == 0 {
            combination.x
        } else {
            (!combination.x) & 0x3
        } as usize;
        let has_count = query.aggregates().contains(&Aggregate::Count);
        let has_sum_price = query.aggregates().contains(&Aggregate::SumPrice);
        let set = AggregatesCombination::from_aggregates_query(&query).set(query.action);
        let bins = match (has_count, has_sum_price) {
            (true, true) => Bins::All,
            (true, false) => Bins::from(["count"]),
            (false, true) => Bins::from(["sum_price"]),
            _ => unreachable!(),
        };
        let batch_reads: Vec<_> = AggregatesCombination::user_key_iter(&query)
            .map(|user_key| {
                let key = as_key!("test", set.as_str(), user_key.as_str());
                BatchRead::new(key, bins.clone())
            })
            .collect();
        match self.aggregates_clients[client_idx]
            .batch_get(&self.batch_policy, batch_reads)
            .await
        {
            Ok(records) => Self::records_to_aggregates_reply(records, query),
            Err(e) => bail!("Could not get aggregate {:?}: {}", query, e),
        }
    }
}
