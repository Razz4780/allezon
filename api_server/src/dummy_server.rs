use anyhow::Context;
use database::{aggregates::AggregatesQuery, user_profiles::UserProfilesQuery};
use std::{net::SocketAddr, str};
use tokio::sync::oneshot::Receiver;
use warp::{
    filters::BoxedFilter, http::StatusCode, hyper::body::Bytes, reply::Response, Filter, Reply,
};

pub struct DummyServer {
    filter: BoxedFilter<(Response,)>,
}

impl Default for DummyServer {
    fn default() -> Self {
        let user_tags = warp::path("user_tags")
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .map(|body: Bytes| {
                let expected = str::from_utf8(body.as_ref());
                log::info!("Expected response for user_tags: {:?}", expected);

                let response = warp::reply::with_status(body.to_vec(), StatusCode::NO_CONTENT);
                let response =
                    warp::reply::with_header(response, "content-type", "application/json");
                response.into_response()
            });

        let user_profiles = warp::path("user_profiles")
            .and(warp::path::param())
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .map(|cookie: String, query: UserProfilesQuery, body: Bytes| {
                let expected = str::from_utf8(body.as_ref());
                log::info!(
                    "Expected response for user_profiles with cookie {} and query {:?}: {:?}",
                    cookie,
                    query,
                    expected
                );

                let response = warp::reply::with_status(body.to_vec(), StatusCode::OK);
                let response =
                    warp::reply::with_header(response, "content-type", "application/json");
                response.into_response()
            });

        let aggregates = warp::path("aggregates")
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .map(|query: Vec<(String, String)>, body: Bytes| {
                if let Some(query) = AggregatesQuery::from_pairs(query) {
                    let expected = str::from_utf8(body.as_ref());
                    log::info!(
                        "Expected response for aggregates with query {:?}: {:?}",
                        query,
                        expected
                    );

                    let response = warp::reply::with_status(body.to_vec(), StatusCode::OK);
                    let response =
                        warp::reply::with_header(response, "content-type", "application/json");
                    response.into_response()
                } else {
                    log::warn!("Invalid aggregates query");
                    StatusCode::BAD_REQUEST.into_response()
                }
            });

        let filter = user_tags.or(user_profiles).unify().or(aggregates).unify();

        Self {
            filter: filter.boxed(),
        }
    }
}

impl DummyServer {
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
