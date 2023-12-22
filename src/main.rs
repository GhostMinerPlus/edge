mod service;

use std::{
    fs,
    io::{self, Error, ErrorKind},
    sync::Arc,
};

use axum::{routing::post, Router};
use serde::Deserialize;
use sqlx::{MySql, Pool};

#[derive(Deserialize)]
struct Config {
    name: String,
    port: u16,
    db_url: String,
}

pub struct AppState {
    pool: Pool<MySql>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("INFO")).init();
    let config: Config = toml::from_str(&fs::read_to_string("config.toml")?)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.message()))?;

    // build our application with a route
    let app = Router::new()
        .route(
            &format!("/{}/execute", config.name),
            post(service::http_execute),
        )
        .with_state(Arc::new(AppState {
            pool: sqlx::Pool::connect(&config.db_url)
                .await
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
        }));

    // run our app with hyper, listening globally on port 3000
    let address = format!("[::]:{}", config.port);
    log::info!("serving at {address}");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
