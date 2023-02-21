use crate::user_profiles::{UserProfilesQuery, UserProfilesReply};
use crate::user_tag::{Action, UserTag};
use aerospike::{
    as_bin, as_key, Bins, Client, ClientPolicy, Error, ErrorKind, Expiration, GenerationPolicy,
    Key, ReadPolicy, Record, ResultCode, Value, WritePolicy,
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
    const NAMESPACE: &str = "test";
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

    pub async fn create_user_tag(&self, user_tag: &UserTag) -> anyhow::Result<()> {
        loop {
            match self.update_user_profile(user_tag.clone()).await {
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

    async fn update_user_profile(&self, user_tag: UserTag) -> anyhow::Result<bool> {
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
}
