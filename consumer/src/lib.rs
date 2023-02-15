pub mod aggregates;
pub mod user_profiles;

use aerospike::{ClientPolicy, Expiration, GenerationPolicy, RecordExistsAction, WritePolicy};
use anyhow::bail;
use std::net::SocketAddr;

pub struct ProcessorCommon {
    pub client: aerospike::Client,
    pub create_policy: WritePolicy,
    pub update_policy: WritePolicy,
}

impl ProcessorCommon {
    pub async fn new(aerospike_addr: SocketAddr) -> anyhow::Result<Self> {
        let mut create_policy = WritePolicy::new(0, Expiration::Never);
        let mut update_policy = create_policy.clone();
        create_policy.generation_policy = GenerationPolicy::ExpectGenEqual;
        create_policy.record_exists_action = RecordExistsAction::CreateOnly;
        update_policy.generation_policy = GenerationPolicy::None;
        update_policy.record_exists_action = RecordExistsAction::UpdateOnly;
        let client_policy = ClientPolicy::default();

        let client = match aerospike::Client::new(&client_policy, &aerospike_addr.to_string()).await
        {
            Ok(client) => client,
            Err(e) => bail!("Could not create client: {}", e),
        };
        Ok(ProcessorCommon {
            client,
            create_policy,
            update_policy,
        })
    }
}
