use anyhow::Context;
use api_server::user_tag::UserTag;
use async_trait::async_trait;
use event_queue::consumer::{EventProcessor, EventStream};
use serde::Deserialize;
use std::{net::SocketAddr, process::ExitCode};
use tokio::{
    signal,
    sync::oneshot::{self, Receiver},
};

struct DummyProcessor;

#[async_trait]
impl EventProcessor for DummyProcessor {
    type Event = UserTag;

    async fn process(&self, event: Self::Event) -> anyhow::Result<()> {
        log::info!("Consuming tag {:?}", event);
        Ok(())
    }
}

#[derive(Deserialize)]
struct Args {
    kafka_brokers: Vec<SocketAddr>,
    kafka_group: String,
    kafka_topic: String,
}

async fn run_consumer(stop: Receiver<()>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to parse config from environment variables")?;
    let stream = EventStream::new(&args.kafka_brokers, args.kafka_group, args.kafka_topic)?;

    tokio::select! {
        res = stream.consume(&DummyProcessor {}) => res,
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
