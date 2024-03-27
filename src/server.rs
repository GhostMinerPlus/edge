//! Server that provides services.

use std::sync::Arc;

use axum::{routing, Router};
use tokio::sync::Mutex;

use crate::{
    app,
    err::{Error, ErrorKind, Result},
    mem_table,
};

mod service;

async fn serve(ip: &str, port: u16, name: &str, db_url: &str) -> Result<()> {
    // build our application with a route
    let app = Router::new()
        .route(
            &format!("/{}/execute", name),
            routing::post(service::http_execute),
        )
        .route(
            &format!("/{}/require", name),
            routing::post(service::http_require),
        )
        .with_state(Arc::new(app::AppState {
            pool: sqlx::Pool::connect(db_url)
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?,
            mem_table: Mutex::new(mem_table::MemTable::new()),
        }));

    // run our app with hyper, listening globally on port 3000
    let address = format!("{}:{}", ip, port);
    log::info!("serving at {address}/{}", name);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    axum::serve(listener, app)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

// Public
pub struct Server {
    ip: String,
    name: String,
    port: u16,
    db_url: String,
}

impl Server {
    pub fn new(ip: String, port: u16, name: String, db_url: String) -> Self {
        Self {
            ip,
            port,
            name,
            db_url,
        }
    }

    pub async fn run(self) -> Result<()> {
        serve(&self.ip, self.port, &self.name, &self.db_url).await
    }
}
