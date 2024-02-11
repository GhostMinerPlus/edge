use std::{
    io::{self, Error, ErrorKind},
    sync::Arc,
};

use axum::{routing, Router};
use earth::AsConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::mem_table::MemTable;

mod app;
mod edge;
mod service;
mod mem_table;
mod data;

#[derive(Debug, Deserialize, Serialize, Clone, AsConfig)]
struct Config {
    ip: String,
    name: String,
    port: u16,
    db_url: String,
    thread_num: u8,
    log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ip: "0.0.0.0".to_string(),
            name: "edge".to_string(),
            port: 80,
            db_url: Default::default(),
            thread_num: 8,
            log_level: "INFO".to_string(),
        }
    }
}

fn main() -> io::Result<()> {
    let mut arg_v: Vec<String> = std::env::args().collect();
    arg_v.remove(0);
    let file_name = if !arg_v.is_empty() && !arg_v[0].starts_with("--") {
        arg_v.remove(0)
    } else {
        "config.toml".to_string()
    };

    let mut config = Config::default();
    config.merge_by_file(&file_name);
    if !arg_v.is_empty() {
        config.merge_by_args(&arg_v);
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log_level))
        .init();
    log::info!("{:?}", config);

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

async fn start_task(_: &Config) -> io::Result<()> {
    Ok(())
}

async fn serve(config: &Config) -> io::Result<()> {
    // build our application with a route
    let app = Router::new()
        .route(
            &format!("/{}/execute", config.name),
            routing::post(service::http_execute),
        )
        .with_state(Arc::new(app::AppState {
            pool: sqlx::Pool::connect(&config.db_url)
                .await
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
            mem_table: Mutex::new(MemTable::new())
        }));

    // run our app with hyper, listening globally on port 3000
    let address = format!("{}:{}", config.ip, config.port);
    log::info!("serving at {address}/{}", config.name);
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await
}
