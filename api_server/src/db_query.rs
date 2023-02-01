use crate::{
    aggregates::{Aggregate, AggregatesRow},
    aggregates::{AggregatesQuery, AggregatesReply},
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json;

pub const AS_REST_ADDR: &str = "http://localhost:8081";
pub const AS_KVS_BASE: &str = "v1/kvs";
pub const AS_QUERY_BASE: &str = "v1/query";
pub const AS_OPERATE_BASE: &str = "v2/operate";
pub const AS_NAMESPACE: &str = "test";
pub const AS_VIEW_PROFILES_SET: &str = "view-profiles";
pub const AS_BUY_PROFILES_SET: &str = "buy-profiles";

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
) -> anyhow::Result<Vec<UserTag>> {
    let set = match action {
        Action::Buy => AS_BUY_PROFILES_SET,
        Action::View => AS_VIEW_PROFILES_SET,
    };
    let url = format!(
        "{}/{}/{}/{}/{}",
        AS_REST_ADDR, AS_KVS_BASE, AS_NAMESPACE, set, cookie
    );

    let res = client.get(url).send().await?;
    anyhow::ensure!(
        res.status() == StatusCode::OK,
        "could not get user tags: {}",
        res.status()
    );

    let text = res.text().await?;
    let resp: UserProfileDbQueryResponse = serde_json::from_str(&text)?;
    let l = query.time_range.from().timestamp_millis();
    let r = query.time_range.to().timestamp_millis();
    let tags: Vec<UserTag> = resp
        .bins
        .user_tags
        .into_iter()
        .skip_while(|&(t, _)| r <= -t)
        .take_while(|&(t, _)| l <= -t)
        .take(query.limit as usize)
        .map(|(_, tag)| tag)
        .collect();
    Ok(tags)
}

pub async fn get_user_profile(
    cookie: String,
    query: &UserProfilesQuery,
) -> anyhow::Result<UserProfilesReply> {
    let client = Client::new();

    let (views, buys) = tokio::try_join!(
        get_user_tags(&client, &cookie, query, Action::View),
        get_user_tags(&client, &cookie, query, Action::Buy)
    )?;

    let reply = UserProfilesReply {
        cookie,
        views,
        buys,
    };
    Ok(reply)
}

#[derive(Serialize)]
struct AggregateDbRequest {
    filter: String,
}

impl AggregateDbRequest {
    fn new(query: &AggregatesQuery) -> Self {
        let min_from = query.time_range.from().timestamp() / 60;
        // Aerospike ranges are inclusive on both sides; we want exclusive from the right.
        let min_to = query.time_range.to().timestamp() / 60 - 1;
        let filter = format!(
            "{{
                \"binName\": \"time\",
                \"type\": \"QUERY_RANGE_FILTER\",
                \"begin\": {},
                \"end\": {},
            }}",
            min_from, min_to
        );
        Self { filter }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregateRecord {
    pub time: i64,
    pub count: usize,
    pub sum_price: usize,
}

#[allow(dead_code, non_snake_case)]
#[derive(Deserialize)]
struct AggregateDbRecord {
    userKey: String,
    generation: usize,
    ttl: i32,
    bins: AggregateRecord,
}

#[derive(Deserialize)]
struct AggregateDbResponse {
    records: Vec<AggregateDbRecord>,
}

pub async fn get_aggregate(query: AggregatesQuery) -> anyhow::Result<AggregatesReply> {
    let has_count = query.aggregates().contains(&Aggregate::Count);
    let has_sum_price = query.aggregates().contains(&Aggregate::SumPrice);
    let set = query.db_set_name();
    let url = format!(
        "{}/{}/{}/{}",
        AS_REST_ADDR, AS_QUERY_BASE, AS_NAMESPACE, set
    );
    let request = AggregateDbRequest::new(&query);
    let client = Client::new();

    let res = client.post(url).json(&request).send().await?;
    anyhow::ensure!(res.status() == StatusCode::OK);

    let text = res.text().await?;
    let resp: AggregateDbResponse = serde_json::from_str(&text)?;
    let mut rows = resp.records;
    rows.sort_by(|a, b| a.bins.time.cmp(&b.bins.time));
    let rows: Vec<_> = rows
        .into_iter()
        .map(|rec| {
            let count = if has_count {
                Some(rec.bins.count)
            } else {
                None
            };
            let sum_price = if has_sum_price {
                Some(rec.bins.sum_price)
            } else {
                None
            };
            AggregatesRow { sum_price, count }
        })
        .collect();

    query.make_reply(rows)
}
