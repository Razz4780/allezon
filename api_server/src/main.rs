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
    aerospike_nodes: Vec<SocketAddr>,
    db_write_timeout_ms: u64,
    db_write_initial_backoff_ms: u64,
    aggr_pusher_interval_ms: u64,
}

#[cfg(feature = "only_echo")]
#[derive(Deserialize, Debug)]
struct Args {
    address: SocketAddr,
}

#[cfg(not(feature = "only_echo"))]
async fn run_server(stop: Receiver<()>) -> anyhow::Result<()> {
    use api_server::{app::App, server::ApiServer};
    use database::{client::SimpleDbClient, retrying_client::RetryingClient};
    use std::{
        sync::{atomic::Ordering, Arc},
        time::Duration,
    };

    let args: Args =
        envy::from_env().context("failed to read configuration from environment variables")?;

    let db_client = RetryingClient::new(
        SimpleDbClient::new(args.aerospike_nodes).await?,
        Duration::from_millis(args.db_write_timeout_ms),
        Duration::from_millis(args.db_write_initial_backoff_ms),
    );
    let app = Arc::new(App::new(db_client));
    let worker = app
        .clone()
        .worker(Duration::from_millis(args.aggr_pusher_interval_ms));
    let stop_flag = worker.stop_flag();
    let worker_task = tokio::spawn(worker.run());

    ApiServer::new(app.clone())
        .run(args.address, stop)
        .await
        .context("api server failed")?;

    stop_flag.store(true, Ordering::Relaxed);
    worker_task.await.context("worker task panicked")
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
