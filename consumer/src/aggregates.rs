use crate::ProcessorCommon;
use aerospike::{as_bin, as_key, operations, Error, ErrorKind, Expiration, ResultCode};
use anyhow::bail;
use api_server::db_query::AggregatesCombination;
use api_server::user_tag::UserTag;
use event_queue::consumer::EventProcessor;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct AggregatesProcessor {
    processor: Arc<ProcessorCommon>,
    first: AggregatesCombination,
    second: AggregatesCombination,
}

impl AggregatesProcessor {
    pub async fn new(
        aerospike_addr: SocketAddr,
        first: AggregatesCombination,
        second: AggregatesCombination,
    ) -> anyhow::Result<Self> {
        let mut processor = ProcessorCommon::new(aerospike_addr).await?;
        processor.create_policy.expiration = Expiration::Seconds(24 * 60 * 60);
        processor.update_policy.expiration = Expiration::Seconds(24 * 60 * 60);
        Ok(Self {
            processor: processor.into(),
            first,
            second,
        })
    }

    async fn add_to_aggregates(&self, price: i64, set: &str, user_key: &str) -> anyhow::Result<()> {
        let key = as_key!("test", set, user_key);
        let count = as_bin!("count", 1);
        let sum_price = as_bin!("sum_price", price);
        let bins = [count.clone(), sum_price.clone()];

        match self
            .processor
            .client
            .put(&self.processor.create_policy, &key, &bins)
            .await
        {
            Ok(_) => Ok(()),
            Err(Error(ErrorKind::ServerError(ResultCode::KeyExistsError), _))
            | Err(Error(ErrorKind::ServerError(ResultCode::GenerationError), _)) => {
                let add_count = operations::add(&count);
                let add_price = operations::add(&sum_price);
                let ops = [add_count, add_price];
                match self
                    .processor
                    .client
                    .operate(&self.processor.update_policy, &key, &ops)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => bail!("Could not update {}/{}: {}", set, user_key, e),
                }
            }
            Err(e) => bail!("Could not add to aggregates {}/{}: {}", set, user_key, e),
        }
    }
}

#[async_trait::async_trait]
impl EventProcessor for AggregatesProcessor {
    type Event = UserTag;

    async fn process(&self, tag: UserTag) -> anyhow::Result<()> {
        let tag_copy = tag.clone();
        tokio::try_join!(
            async move {
                let set = self.first.set(tag.action);
                let user_key = self.first.user_key_from_tag(&tag);
                self.add_to_aggregates(
                    tag.product_info.price.into(),
                    set.as_str(),
                    user_key.as_str(),
                )
                .await
            },
            async move {
                let set = self.second.set(tag_copy.action);
                let user_key = self.second.user_key_from_tag(&tag_copy);
                self.add_to_aggregates(
                    tag_copy.product_info.price.into(),
                    set.as_str(),
                    user_key.as_str(),
                )
                .await
            },
        )
        .map(|_| ())
    }
}
