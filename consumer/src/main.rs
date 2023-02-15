use anyhow::Context;
use event_queue::consumer::EventStream;
use serde::Deserialize;
use std::net::SocketAddr;
use std::process::ExitCode;
use tokio::sync::oneshot::Receiver;
use tokio::{signal, sync::oneshot};

#[cfg(not(feature = "aggregates"))]
#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group: String,
    kafka_topic: String,
    aerospike: SocketAddr,
}

#[cfg(feature = "aggregates")]
#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group: String,
    kafka_topic: String,
    aerospike: SocketAddr,
    aggregates_group: u8,
}

#[cfg(not(feature = "aggregates"))]
async fn run_consumer(stop: Receiver<()>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;

    let processor = consumer::user_profiles::UserProfilesProcessor::new(args.aerospike).await?;
    let stream = EventStream::new(&args.kafka_brokers, args.kafka_group, args.kafka_topic)?;

    tokio::select! {
        res = stream.consume(&processor) => res,
        _ = stop => Ok (()),
    }
}

#[cfg(feature = "aggregates")]
async fn run_consumer(stop: Receiver<()>) -> anyhow::Result<()> {
    use api_server::db_query::AggregatesCombination;

    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;
    anyhow::ensure!(
        args.aggregates_group & !0x3 == 0,
        "invalid aggregates_group"
    );
    let first = AggregatesCombination {
        x: args.aggregates_group,
    };
    let second = AggregatesCombination {
        x: (!args.aggregates_group) & 0x7,
    };
    let processor =
        consumer::aggregates::AggregatesProcessor::new(args.aerospike, first, second).await?;
    let stream = EventStream::new(&args.kafka_brokers, args.kafka_group, args.kafka_topic)?;

    tokio::select! {
        res = stream.consume(&processor) => res,
        _ = stop => Ok (()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let (tx, rx) = oneshot::channel();
    let res = tokio::try_join!(
        async move {
            signal::ctrl_c()
                .await
                .context("failed to listen for ctrl-c")?;
            log::info!("Received a ctrl-c signal");
            tx.send(()).ok();
            Ok(())
        },
        run_consumer(rx),
    );

    match res {
        Ok(..) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("An error occurred: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
