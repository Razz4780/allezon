mod db_client;
mod server;
mod time_range;
mod user_profiles;
mod user_tag;

use anyhow::Context;
use db_client::DbClient;
use serde::Deserialize;
use server::ApiServer;
use std::{net::SocketAddr, process::ExitCode};
use tokio::{
    signal,
    sync::oneshot::{self, Receiver},
};

#[derive(Deserialize, Debug)]
struct Args {
    server_addr: SocketAddr,
    aerospike_addr: SocketAddr,
}

async fn run_server(stop: Receiver<()>) -> anyhow::Result<()> {
    let args: Args =
        envy::from_env().context("failed to read configuration from environment variables")?;

    let db_client = DbClient::new(args.aerospike_addr).await?;

    ApiServer::new(db_client).run(args.server_addr, stop).await
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
