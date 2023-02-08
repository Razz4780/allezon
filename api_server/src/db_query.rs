use crate::{
    aggregates::{Aggregate, AggregatesRow},
    aggregates::{AggregatesQuery, AggregatesReply},
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{ser::SerializeMap, Deserialize, Serialize};
use serde_json;
use std::net::SocketAddr;

pub const AS_KVS_BASE: &str = "v1/kvs";
pub const AS_QUERY_BASE: &str = "v1/query";
pub const AS_OPERATE_BASE: &str = "v2/operate";
pub const AS_INDEX_BASE: &str = "v1/index";
pub const AS_NAMESPACE: &str = "test";
pub const AS_VIEW_PROFILES_SET: &str = "view-profiles";
pub const AS_BUY_PROFILES_SET: &str = "buy-profiles";
pub const AS_QUERY_PARAMS: &str = "replica=MASTER_PROLES";

pub fn time_to_int_user_profile(time: &DateTime<Utc>) -> i64 {
    -time.timestamp_millis()
}

pub fn time_to_int_aggregate(time: &DateTime<Utc>) -> i64 {
    time.timestamp() / 60
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserProfileRecord {
    pub user_tags: Vec<(i64, UserTag)>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UserProfileDbQueryResponse {
    generation: usize,
    ttl: i32,
    bins: UserProfileRecord,
}

async fn get_user_tags(
    client: &Client,
    cookie: &String,
    query: &UserProfilesQuery,
    action: Action,
    db_addr: SocketAddr,
) -> anyhow::Result<Vec<UserTag>> {
    let set = match action {
        Action::Buy => AS_BUY_PROFILES_SET,
        Action::View => AS_VIEW_PROFILES_SET,
    };
    let url = format!(
        "http://{}/{}/{}/{}/{}?{}",
        db_addr, AS_KVS_BASE, AS_NAMESPACE, set, cookie, AS_QUERY_PARAMS,
    );

    let res = client.get(url).send().await?;
    match res.status() {
        StatusCode::OK => {
            let text = res.text().await?;
            let resp: UserProfileDbQueryResponse = serde_json::from_str(&text)?;
            let l = time_to_int_user_profile(query.time_range.from());
            let r = time_to_int_user_profile(query.time_range.to());
            let tags: Vec<UserTag> = resp
                .bins
                .user_tags
                .into_iter()
                .skip_while(|&(t, _)| t <= r)
                .take_while(|&(t, _)| t <= l)
                .take(query.limit as usize)
                .map(|(_, tag)| tag)
                .collect();
            Ok(tags)
        }
        StatusCode::NOT_FOUND => {
            // no tags for this set/key
            Ok(Vec::new())
        }
        _ => anyhow::bail!(
            "could not get {} user tags: {} {:?}",
            cookie,
            res.status(),
            res.text().await?
        ),
    }
}

pub async fn get_user_profile(
    cookie: String,
    query: &UserProfilesQuery,
    db_addr: SocketAddr,
) -> anyhow::Result<UserProfilesReply> {
    let client = Client::new();

    let (views, buys) = tokio::try_join!(
        get_user_tags(&client, &cookie, query, Action::View, db_addr),
        get_user_tags(&client, &cookie, query, Action::Buy, db_addr)
    )?;

    let reply = UserProfilesReply {
        cookie,
        views,
        buys,
    };
    Ok(reply)
}

enum DbFilter {
    QueryRangeFilter {
        bin_name: String,
        begin: i64,
        end: i64,
    },
}

impl Serialize for DbFilter {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DbFilter::QueryRangeFilter {
                bin_name,
                begin,
                end,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("type", "RANGE")?;
                map.serialize_entry("binName", bin_name)?;
                map.serialize_entry("begin", begin)?;
                map.serialize_entry("end", end)?;
                map.end()
            }
        }
    }
}

#[derive(Serialize)]
struct AggregateDbRequest {
    filter: DbFilter,
}

impl AggregateDbRequest {
    fn new(query: &AggregatesQuery) -> Self {
        let begin = time_to_int_aggregate(query.time_range.from());
        // Aerospike ranges are inclusive on both sides; we want exclusive from the right.
        let end = time_to_int_aggregate(query.time_range.to()) - 1;
        let filter = DbFilter::QueryRangeFilter {
            bin_name: "time".into(),
            begin,
            end,
        };
        Self { filter }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregateRecord {
    pub time: i64,
    pub count: usize,
    pub sum_price: usize,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct AggregateDbRecord {
    generation: usize,
    ttl: i32,
    bins: AggregateRecord,
}

#[derive(Deserialize)]
struct AggregateDbResponse {
    records: Vec<AggregateDbRecord>,
}

pub async fn get_aggregate(
    query: AggregatesQuery,
    db_addr: SocketAddr,
) -> anyhow::Result<AggregatesReply> {
    let has_count = query.aggregates().contains(&Aggregate::Count);
    let has_sum_price = query.aggregates().contains(&Aggregate::SumPrice);
    let set = query.db_set_name();
    let url = format!(
        "http://{}/{}/{}/{}?{}",
        db_addr, AS_QUERY_BASE, AS_NAMESPACE, set, AS_QUERY_PARAMS
    );
    let request = AggregateDbRequest::new(&query);
    let client = Client::new();

    let res = client.post(url).json(&request).send().await?;
    let records = match res.status() {
        StatusCode::OK => {
            let text = res.text().await?;
            let resp: AggregateDbResponse = serde_json::from_str(&text)?;
            let mut records = resp.records;
            records.sort_by(|a, b| a.bins.time.cmp(&b.bins.time));
            records
        }
        StatusCode::NOT_FOUND => Vec::new(),
        _ => {
            anyhow::bail!(
                "could not get {:?} aggregate: {} {:?}",
                query,
                res.status(),
                res.text().await?
            );
        }
    };
    let mut rows = Vec::new();
    let mut records_idx = 0;
    for time in query.time_range.bucket_starts() {
        let time = time_to_int_aggregate(&time);
        let row = if records.len() <= records_idx || (time < records[records_idx].bins.time) {
            let count = has_count.then_some(0);
            let sum_price = has_sum_price.then_some(0);
            AggregatesRow { sum_price, count }
        } else {
            let count = has_count.then_some(records[records_idx].bins.count);
            let sum_price = has_sum_price.then_some(records[records_idx].bins.sum_price);
            records_idx += 1;
            AggregatesRow { count, sum_price }
        };
        rows.push(row);
    }
    query.make_reply(rows)
}
