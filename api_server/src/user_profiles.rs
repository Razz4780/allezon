use crate::{time_range::SimpleTimeRange, user_tag::UserTag};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub struct UserProfilesQuery {
    pub time_range: SimpleTimeRange,
    #[serde(default = "UserProfilesQuery::default_limit")]
    pub limit: u32,
}

impl UserProfilesQuery {
    fn default_limit() -> u32 {
        200
    }
}

#[derive(Serialize)]
pub struct UserProfilesReply {
    pub cookie: String,
    pub views: Vec<UserTag>,
    pub buys: Vec<UserTag>,
}
