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

pub struct DbClient {
    client: Client,
    read_policy: ReadPolicy,
    batch_policy: BatchPolicy,
}

impl DbClient {
    pub async fn new(db_addr: SocketAddr) -> anyhow::Result<Self> {
        let read_policy = ReadPolicy::default();
        let batch_policy = BatchPolicy::default();
        match Client::new(&ClientPolicy::default(), &db_addr.to_string()).await {
            Ok(client) => Ok(Self {
                client,
                read_policy,
                batch_policy,
            }),
            Err(e) => bail!("Could not create db client: {}", e),
        }
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
            .client
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
        let has_count = query.aggregates().contains(&Aggregate::Count);
        let has_sum_price = query.aggregates().contains(&Aggregate::SumPrice);
        let set = query.db_set_name();
        let bins = match (has_count, has_sum_price) {
            (true, true) => Bins::All,
            (true, false) => Bins::from(["count"]),
            (false, true) => Bins::from(["sum_price"]),
            _ => unreachable!(),
        };
        let batch_reads: Vec<_> = query
            .db_user_keys()
            .map(|user_key| {
                let key = as_key!("test", set.as_str(), user_key.as_str());
                BatchRead::new(key, bins.clone())
            })
            .collect();
        match self.client.batch_get(&self.batch_policy, batch_reads).await {
            Ok(records) => Self::records_to_aggregates_reply(records, query),
            Err(e) => bail!("Could not get aggregate {:?}: {}", query, e),
        }
    }
}
