use crate::db_client::DbClient;
use crate::user_profiles::UserProfilesQuery;
use crate::user_tag::UserTag;
use anyhow::Context;
use std::net::SocketAddr;
use tokio::sync::oneshot::Receiver;
use warp::{filters::BoxedFilter, http::StatusCode, reply::Response, Filter, Reply};

pub struct ApiServer {
    filter: BoxedFilter<(Response,)>,
}

impl ApiServer {
    async fn user_tags(db_client: DbClient, user_tag: UserTag) -> Response {
        match db_client.create_user_tag(&user_tag).await {
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

    async fn user_profiles(
        db_client: DbClient,
        cookie: String,
        query: UserProfilesQuery,
        _body: warp::hyper::body::Bytes,
    ) -> Response {
        match db_client.get_user_profile(cookie, query).await {
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

    pub fn new(db_client: DbClient) -> Self {
        let with_state = warp::any().map(move || db_client.clone());

        let user_tags = warp::path("user_tags")
            .and(warp::path::end())
            .and(warp::post())
            .and(with_state.clone())
            .and(warp::body::json())
            .then(Self::user_tags);

        let user_profiles = warp::path("user_profiles")
            .and(with_state)
            .and(warp::path::param())
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .then(Self::user_profiles);

        let filter = user_tags.or(user_profiles).unify().boxed();

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
