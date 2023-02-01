use crate::{
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::{Action, UserTag},
};

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json;

const AS_REST_ADDR: &str = "http://localhost:8081";
const AS_KVS_BASE: &str = "v1/kvs";
const AS_NAMESPACE: &str = "test";
const AS_VIEW_PROFILES_SET: &str = "view-profiles";
const AS_BUY_PROFILES_SET: &str = "buy-profiles";

#[derive(Serialize, Deserialize)]
pub struct UserProfileDb {
    user_tags: Vec<(i64, UserTag)>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UserProfileDbQueryResponse {
    generation: usize,
    ttl: i32,
    bins: UserProfileDb,
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
        .rev()
        .skip_while(|&(t, _)| t < l)
        .take_while(|&(t, _)| t < r)
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
