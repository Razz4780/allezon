use anyhow::Context;
use serde::Deserialize;
use std::{net::SocketAddr, process::ExitCode};
use tokio::{
    signal,
    sync::oneshot::{self, Receiver},
};

#[cfg(not(feature = "only_echo"))]
#[derive(Deserialize, Debug)]
struct Args {
    address: SocketAddr,
    kafka_brokers: Vec<SocketAddr>,
    kafka_topic: String,
}

#[cfg(feature = "only_echo")]
#[derive(Deserialize, Debug)]
struct Args {
    address: SocketAddr,
}

#[cfg(not(feature = "only_echo"))]
async fn run_server(stop: Receiver<()>) -> anyhow::Result<()> {
    use api_server::{app::App, server::ApiServer};
    use event_queue::producer::EventProducer;

    let args: Args =
        envy::from_env().context("failed to read configuration from environment variables")?;

    let producer = EventProducer::new(&args.kafka_brokers, args.kafka_topic)?;
    let app = App::new(producer);

    ApiServer::new(app.into()).run(args.address, stop).await
}

#[cfg(feature = "only_echo")]
async fn run_server(stop: Receiver<()>) -> anyhow::Result<()> {
    use api_server::dummy_server::DummyServer;

    let args: Args =
        envy::from_env().context("failed to read configuration from environment variables")?;

    DummyServer::default().run(args.address, stop).await
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
        run_server(rx),
    );

    match res {
        Ok(..) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("An error occurred: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
