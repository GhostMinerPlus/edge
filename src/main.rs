mod service;
mod task;
mod util;

use std::{
    fs,
    io::{self, Error, ErrorKind},
    sync::Arc,
};

use axum::{routing, Router};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
struct Config {
    ip: String,
    name: String,
    port: u16,
    db_url: String,
    thread_num: u8,
    host_v: Vec<String>,
}

fn main() -> io::Result<()> {
    let arg_v: Vec<String> = std::env::args().collect();
    let file_name = if arg_v.len() == 2 {
        arg_v[1].as_str()
    } else {
        "config.toml"
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("INFO")).init();
    let config_s = fs::read_to_string(file_name)?;
    let config: Config =
        toml::from_str(&config_s).map_err(|e| Error::new(io::ErrorKind::Other, e.message()))?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.thread_num as usize)
        .build()?
        .block_on(async move {
            start_task(&config).await?;
            serve(&config).await
        })?;
    Ok(())
}

async fn start_task(config: &Config) -> io::Result<()> {
    let config_copy = config.clone();
    log::info!("starting task 'report_ipv6'");
    tokio::spawn(async move {
        loop {
            task::report_address6(&config_copy.name, config_copy.port, &config_copy.host_v).await;
        }
    });
    Ok(())
}

async fn serve(config: &Config) -> io::Result<()> {
    // build our application with a route
    let app = Router::new()
        .route(
            &format!("/{}/execute", config.name),
            routing::post(service::http_execute),
        )
        .with_state(Arc::new(util::AppState {
            pool: sqlx::Pool::connect(&config.db_url)
                .await
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
        }));

    // run our app with hyper, listening globally on port 3000
    let address = format!("{}:{}", config.ip, config.port);
    log::info!("serving at {address}");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
