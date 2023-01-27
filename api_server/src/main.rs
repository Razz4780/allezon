use anyhow::Context;
use clap::Parser;
use std::{net::SocketAddr, process::ExitCode};
use tokio::{
    signal,
    sync::oneshot::{self, Receiver},
};

#[derive(Parser)]
struct Args {
    /// Address of the socket this server will listen on.
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    address: SocketAddr,
}

#[cfg(not(feature = "only_echo"))]
async fn run_server(args: Args, stop: Receiver<()>) -> anyhow::Result<()> {
    use api_server::server::ApiServer;

    ApiServer::default().run(args.address, stop).await
}

#[cfg(feature = "only_echo")]
async fn run_server(args: Args, stop: Receiver<()>) -> anyhow::Result<()> {
    use api_server::dummy_server::DummyServer;

    DummyServer::default().run(args.address, stop).await
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
        run_server(args, rx),
    );

    match res {
        Ok(..) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("An error occurred: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
