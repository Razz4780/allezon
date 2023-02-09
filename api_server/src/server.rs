use crate::{
    aggregates::AggregatesQuery, app::App, db_query::DbClient, user_profiles::UserProfilesQuery,
    user_tag::UserTag,
};
use anyhow::Context;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::oneshot::Receiver;
use warp::{
    filters::BoxedFilter, http::StatusCode, hyper::body::Bytes, reply::Response, Filter, Reply,
};

pub struct ApiServer {
    filter: BoxedFilter<(Response,)>,
}

impl ApiServer {
    pub fn new(app: Arc<App>, db_client: Arc<DbClient>) -> Self {
        let user_tags = warp::path("user_tags")
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::json())
            .then(move |user_tag: UserTag| {
                let app = app.clone();
                async move {
                    match app.send_tag(&user_tag).await {
                        Ok(()) => {
                            let response = warp::reply::json(&user_tag);
                            let response =
                                warp::reply::with_status(response, StatusCode::NO_CONTENT);
                            let response = warp::reply::with_header(
                                response,
                                "content-type",
                                "application/json",
                            );
                            response.into_response()
                        }
                        Err(e) => {
                            log::error!("Failed to send user tag to Kafka: {:?}", e);
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                }
            });

        let db_client_copy = db_client.clone();
        let user_profiles = warp::path("user_profiles")
            .and(warp::path::param())
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .then(
                move |cookie: String, query: UserProfilesQuery, _body: Bytes| {
                    let db_client_copy = db_client_copy.clone();
                    async move {
                        match db_client_copy.get_user_profile(cookie, &query).await {
                            Ok(reply) => {
                                let response = warp::reply::json(&reply);
                                let response = warp::reply::with_status(response, StatusCode::OK);
                                let response = warp::reply::with_header(
                                    response,
                                    "content-type",
                                    "application/json",
                                );
                                response.into_response()
                            }
                            Err(e) => {
                                log::error!("Failed to query database: {}, {:?}", e, query);
                                StatusCode::INTERNAL_SERVER_ERROR.into_response()
                            }
                        }
                    }
                },
            );

        let aggregates = warp::path("aggregates")
            .and(warp::query())
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .then(move |query: Vec<(String, String)>, _body: Bytes| {
                let db_client_copy = db_client.clone();
                async move {
                    if let Some(query) = AggregatesQuery::from_pairs(query) {
                        match db_client_copy.get_aggregate(query).await {
                            Ok(reply) => {
                                let response = warp::reply::json(&reply);
                                let response = warp::reply::with_status(response, StatusCode::OK);
                                let response = warp::reply::with_header(
                                    response,
                                    "content-type",
                                    "application/json",
                                );
                                response.into_response()
                            }
                            Err(e) => {
                                log::error!("Failed to query database: {}", e);
                                StatusCode::INTERNAL_SERVER_ERROR.into_response()
                            }
                        }
                    } else {
                        StatusCode::BAD_REQUEST.into_response()
                    }
                }
            });

        let filter = user_tags.or(user_profiles).unify().or(aggregates).unify();

        Self {
            filter: filter.boxed(),
        }
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
