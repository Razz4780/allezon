use anyhow::Context;
use api_server::server::ApiServer;
use clap::Parser;
use std::{net::SocketAddr, process::ExitCode};
use tokio::{signal, sync::oneshot};

#[derive(Parser)]
struct Args {
    /// Address of the socket this server will listen on.
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    address: SocketAddr,
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let args = Args::parse();

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
        ApiServer::default().run(args.address, rx),
    );

    match res {
        Ok(..) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("An error occurred: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
