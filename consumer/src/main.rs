use anyhow::Context;
use consumer::{
    aggregates::{AggregatesFilter, AggregatesProcessor},
    user_profiles::UserProfilesProcessor,
};
use database::{client::SimpleDbClient, retrying_client::RetryingClient};
use event_queue::consumer::EventStream;
use serde::Deserialize;
use std::{net::SocketAddr, process::ExitCode, time::Duration};
use tokio::{
    signal,
    sync::watch::{self, Receiver},
    task,
};

#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group_base: String,
    kafka_topic: String,
    aerospike_nodes: Vec<SocketAddr>,
    update_retry_limit_ms: u64,
}

async fn run_consumers(mut stop: Receiver<bool>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;

    let db_client = RetryingClient::new(
        SimpleDbClient::new(args.aerospike_nodes).await?,
        Duration::from_millis(args.update_retry_limit_ms),
    );
    let filters = AggregatesFilter::all();

    let mut tasks = Vec::with_capacity(filters.len() + 1);

    let processor = UserProfilesProcessor::new(db_client.clone());
    let mut stream = EventStream::new(
        &args.kafka_brokers,
        format!("{}-profiles", args.kafka_group_base),
        &args.kafka_topic,
        stop.clone(),
    )?;
    tasks.push(task::spawn(async move { stream.consume(&processor).await }));

    for filter in filters {
        let processor = AggregatesProcessor::new(filter, db_client.clone());
        let mut stream = EventStream::new(
            &args.kafka_brokers,
            format!("{}-aggregates-{}", args.kafka_group_base, filter),
            &args.kafka_topic,
            stop.clone(),
        )?;
        tasks.push(task::spawn(async move { stream.consume(&processor).await }));
    }

    while !*stop.borrow() {
        stop.changed().await.ok();
    }

    for task in tasks {
        task.await.ok();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let (tx, rx) = watch::channel(false);
    let res = tokio::try_join!(
        async move {
            signal::ctrl_c()
                .await
                .context("failed to listen for ctrl-c")?;
            log::info!("Received a ctrl-c signal");
            tx.send(true).ok();
            Ok(())
        },
        run_consumers(rx),
    );

    match res {
        Ok(..) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("An error occurred: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
