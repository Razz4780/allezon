use crate::ProcessorCommon;
use aerospike::operations::lists::{self, ListOrderType, ListPolicy, ListWriteFlags};
use aerospike::{as_bin, as_key, as_list, as_val, Error, ErrorKind, ResultCode};
use anyhow::bail;
use api_server::db_query::time_to_int_user_profile;
use api_server::user_tag::{Action, UserTag};
use event_queue::consumer::EventProcessor;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct UserProfilesProcessor {
    processor: Arc<ProcessorCommon>,
}

impl UserProfilesProcessor {
    pub async fn new(aerospike_addr: SocketAddr) -> anyhow::Result<Self> {
        let processor = Arc::new(ProcessorCommon::new(aerospike_addr).await?);

        Ok(Self { processor })
    }
}

#[async_trait::async_trait]
impl EventProcessor for UserProfilesProcessor {
    type Event = UserTag;

    async fn process(&self, tag: UserTag) -> anyhow::Result<()> {
        let time = time_to_int_user_profile(&tag.time);
        let tag_str = serde_json::to_string(&tag)?;
        let pair = as_list!(time, tag_str);
        let list = as_list!(pair.clone());
        let bin = as_bin!("user_tags", list);
        let key = match tag.action {
            Action::View => as_key!("test", "view-profiles", tag.cookie.clone()),
            Action::Buy => as_key!("test", "buy-profiles", tag.cookie.clone()),
        };
        match self
            .processor
            .client
            .put(&self.processor.create_policy, &key, &[bin])
            .await
        {
            Ok(_) => Ok(()),
            Err(Error(ErrorKind::ServerError(ResultCode::KeyExistsError), _))
            | Err(Error(ErrorKind::ServerError(ResultCode::GenerationError), _)) => {
                let set_order = lists::set_order("user_tags", ListOrderType::Ordered, &[]);
                let list_policy = ListPolicy {
                    attributes: ListOrderType::Ordered,
                    flags: ListWriteFlags::Default,
                };
                let append = lists::append(&list_policy, "user_tags", &pair);
                let trim = lists::trim("user_tags", 0, 200);
                let ops = vec![set_order, append, trim];

                match self
                    .processor
                    .client
                    .operate(&self.processor.update_policy, &key, &ops)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => bail!("Could not update {}: {}", tag.cookie, e),
                }
            }
            Err(e) => bail!("Could not add user tag {:?} to db: {}", tag, e),
        }
    }
}
