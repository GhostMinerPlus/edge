//! Server that provides services.

use std::{io, sync::Arc};

use axum::{extract::State, http::StatusCode, routing, Router};
use edge_lib::{data::AsDataManager, AsEdgeEngine, EdgeEngine};

// Public
pub struct HttpServer {
    dm: Box<dyn AsDataManager>,
}

impl HttpServer {
    async fn execute(dm: Box<dyn AsDataManager>, script_vn: String) -> io::Result<String> {
        log::info!("executing");
        log::debug!("executing {script_vn}");
        let mut edge_engine = EdgeEngine::new(dm);
        let rs = edge_engine
            .execute(&json::parse(&script_vn).unwrap())
            .await?;
        edge_engine.commit().await?;
        log::info!("commited");
        Ok(rs.dump())
    }

    async fn http_execute(
        State(dm): State<Arc<Box<dyn AsDataManager>>>,
        script_vn: String,
    ) -> (StatusCode, String) {
        match Self::execute(dm.divide(), script_vn).await {
            Ok(s) => (StatusCode::OK, s),
            Err(e) => {
                log::warn!("when http_execute:\n{e}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    pub fn new(dm: Box<dyn AsDataManager>) -> Self {
        Self { dm }
    }

    pub async fn run(self) -> io::Result<()> {
        let mut edge_engine = EdgeEngine::new(self.dm.divide());

        let script = [
            "$->$output = = root->name _",
            "$->$output += = root->ip _",
            "$->$output += = root->port _",
            "info",
        ]
        .join("\\n");
        let rs = edge_engine
            .execute(&json::parse(&format!("{{\"{script}\": null}}")).unwrap())
            .await?;
        log::debug!("{rs}");
        let name = rs["info"][0].as_str().unwrap();
        let ip = rs["info"][1].as_str().unwrap();
        let port = rs["info"][2].as_str().unwrap();
        // build our application with a route
        let app = Router::new()
            .route(
                &format!("/{}/execute", name),
                routing::post(HttpServer::http_execute),
            )
            .with_state(Arc::new(self.dm));
        // run our app with hyper, listening globally on port 3000
        let address = format!("{}:{}", ip, port);
        log::info!("serving at {address}/{}", name);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await
    }
}
