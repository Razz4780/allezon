use crate::{
    user_profiles::{UserProfilesQuery, UserProfilesReply},
    user_tag::UserTag,
};
use anyhow::Context;
use std::net::SocketAddr;
use tokio::sync::oneshot::Receiver;
use warp::{filters::BoxedFilter, http::StatusCode, reply::Response, Filter, Reply};

pub struct ApiServer {
    filter: BoxedFilter<(Response,)>,
}

impl Default for ApiServer {
    fn default() -> Self {
        let user_tags = warp::path("user_tags")
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::json())
            .map(|user_tag: UserTag| {
                // TODO push user tag to Kafka

                let response = warp::reply::json(&user_tag);
                let response = warp::reply::with_status(response, StatusCode::NO_CONTENT);
                let response =
                    warp::reply::with_header(response, "content-type", "application/json");
                response.into_response()
            });

        let user_profiles = warp::path("user_profiles")
            .and(warp::path::param())
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .map(|cookie: String, _query: UserProfilesQuery| {
                // TODO query database for results

                let response = UserProfilesReply {
                    cookie,
                    views: Default::default(),
                    buys: Default::default(),
                };
                let response = warp::reply::json(&response);
                let response = warp::reply::with_status(response, StatusCode::OK);
                let response =
                    warp::reply::with_header(response, "content-type", "application-json");
                response.into_response()
            });

        let filter = user_tags.or(user_profiles).unify();

        Self {
            filter: filter.boxed(),
        }
    }
}

impl ApiServer {
    pub async fn run(self, socket: SocketAddr, stop: Receiver<()>) -> anyhow::Result<()> {
        let stop = async move {
            stop.await.ok();
        };

        let (socket, fut) = warp::serve(self.filter)
            .try_bind_with_graceful_shutdown(socket, stop)
            .context("failed to start the server")?;
        log::info!("Server listening on socket {}", socket);

        fut.await;

        Ok(())
    }
}
