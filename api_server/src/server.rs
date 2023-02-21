use crate::app::App;
use anyhow::Context;
use database::{
    aggregates::AggregatesQuery, client::DbClient, user_profiles::UserProfilesQuery,
    user_tag::UserTag,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::oneshot::Receiver;
use warp::{filters::BoxedFilter, http::StatusCode, reply::Response, Filter, Reply};

pub struct ApiServer {
    filter: BoxedFilter<(Response,)>,
}

impl ApiServer {
    async fn create_tag<C: DbClient>(app: Arc<App<C>>, user_tag: UserTag) -> Response {
        match app.create_user_tag(&user_tag).await {
            Ok(()) => {
                let response = warp::reply::json(&user_tag);
                let response = warp::reply::with_status(response, StatusCode::NO_CONTENT);
                let response =
                    warp::reply::with_header(response, "content-type", "application/json");

                response.into_response()
            }
            Err(e) => {
                log::error!("Failed to create user tag {:?}: {:?}", user_tag, e);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }

    async fn get_user_profile<C: DbClient>(
        app: Arc<App<C>>,
        cookie: String,
        query: UserProfilesQuery,
    ) -> Response {
        match app.get_user_profile(cookie, query).await {
            Ok(reply) => {
                let response = warp::reply::json(&reply);
                let response = warp::reply::with_status(response, StatusCode::OK);
                let response =
                    warp::reply::with_header(response, "content-type", "application/json");

                response.into_response()
            }
            Err(e) => {
                log::error!("Failed to get user profile: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }

    async fn get_aggregates<C: DbClient>(
        app: Arc<App<C>>,
        query: Vec<(String, String)>,
    ) -> Response {
        let Some(query) = AggregatesQuery::from_pairs(query) else {
            return StatusCode::BAD_REQUEST.into_response();
        };

        match app.get_aggregates(query).await {
            Ok(reply) => {
                let response = warp::reply::json(&reply);
                let response = warp::reply::with_status(response, StatusCode::OK);
                let response =
                    warp::reply::with_header(response, "content-type", "application/json");

                response.into_response()
            }
            Err(e) => {
                log::error!("Failed to get aggregates: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }

    pub fn new<C: 'static + DbClient + Send + Sync>(app: Arc<App<C>>) -> Self {
        let with_state = warp::any().map(move || app.clone());

        let user_tags = warp::path("user_tags")
            .and(warp::path::end())
            .and(warp::post())
            .and(with_state.clone())
            .and(warp::body::json())
            .then(Self::create_tag);

        let user_profiles = warp::path("user_profiles")
            .and(with_state.clone())
            .and(warp::path::param())
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .then(Self::get_user_profile);

        let aggregates = warp::path("aggregates")
            .and(with_state)
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .then(Self::get_aggregates);

        let filter = user_tags
            .or(user_profiles)
            .unify()
            .or(aggregates)
            .unify()
            .boxed();

        Self { filter }
    }

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
