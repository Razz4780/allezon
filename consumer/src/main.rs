use anyhow::Context;
use consumer::aggregates::{AggregatesFilter, AggregatesProcessor};
use consumer::user_profiles::UserProfilesProcessor;
use database::client::DbClient;
use event_queue::consumer::EventStream;
use serde::Deserialize;
use std::net::SocketAddr;
use std::process::ExitCode;
use tokio::sync::watch::{self, Receiver};
use tokio::{signal, task};

#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group_base: String,
    kafka_topic: String,
    aerospike_nodes: Vec<SocketAddr>,
}

async fn run_consumers(mut stop: Receiver<bool>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;

    let db_client = DbClient::new(args.aerospike_nodes).await?;
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
