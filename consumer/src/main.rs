use anyhow::Context;
use event_queue::consumer::EventStream;
use serde::Deserialize;
use std::net::SocketAddr;
use std::process::ExitCode;
use tokio::sync::oneshot::Receiver;
use tokio::{signal, sync::oneshot};

#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group: String,
    kafka_topic: String,
    aerospike: SocketAddr,
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
    let (first, second) = match args.kafka_group.rsplit_once("_") {
        Some((_, x)) => {
            let x = u8::from_str_radix(x, 10).context("invalid aggregates group")?;
            (
                AggregatesCombination { x },
                AggregatesCombination { x: (!x) & 0x7 },
            )
        }
        _ => anyhow::bail!("invalid aggregates group"),
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
