//! Server that provides services.
mod crypto;
mod service;

use std::{io, sync::Arc};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing, Json, Router,
};
use edge_lib::{data::AsDataManager, AsEdgeEngine, EdgeEngine, ScriptTree};

async fn http_register(
    State(dm): State<Arc<Box<dyn AsDataManager>>>,
    Json(auth): Json<crypto::Auth>,
) -> (StatusCode, String) {
    match service::register(dm.divide(), &auth).await {
        Ok(_) => (StatusCode::OK, format!("success")),
        Err(e) => {
            log::warn!("when http_register:\n{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

async fn http_login(
    State(dm): State<Arc<Box<dyn AsDataManager>>>,
    Json(auth): Json<crypto::Auth>,
) -> Response<String> {
    match service::login(dm.divide(), &auth).await {
        Ok(token) => Response::builder()
            .header("Set-Cookie", format!("token={token}"))
            .status(StatusCode::OK)
            .body(format!("success"))
            .unwrap(),
        Err(e) => {
            log::warn!("when http_login:\n{e}");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
                .unwrap()
        }
    }
}

async fn http_parse_token(
    hm: HeaderMap,
    State(dm): State<Arc<Box<dyn AsDataManager>>>,
) -> (StatusCode, String) {
    let cookie = match service::get_cookie(&hm) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("when http_parse_token:\n{e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
    };
    match service::parse_auth(&mut *dm.divide(), &cookie).await {
        Ok(s) => (StatusCode::OK, serde_json::to_string(&s).unwrap()),
        Err(e) => {
            log::warn!("when http_parse_token:\n{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

async fn http_execute(
    hm: HeaderMap,
    State(dm): State<Arc<Box<dyn AsDataManager>>>,
    script_vn: String,
) -> (StatusCode, String) {
    match service::execute(dm.divide(), &hm, script_vn).await {
        Ok(s) => (StatusCode::OK, s),
        Err(e) => {
            log::warn!("when http_execute:\n{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

async fn http_execute1(
    hm: HeaderMap,
    State(dm): State<Arc<Box<dyn AsDataManager>>>,
    script_vn: String,
) -> (StatusCode, String) {
    match service::execute1(dm.divide(), &hm, script_vn).await {
        Ok(s) => (StatusCode::OK, s),
        Err(e) => {
            log::warn!("when http_execute:\n{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

// Public
pub struct HttpServer {
    dm: Box<dyn AsDataManager>,
}

impl HttpServer {
    pub fn new(dm: Box<dyn AsDataManager>) -> Self {
        Self { dm }
    }

    pub async fn run(self) -> io::Result<()> {
        let mut edge_engine = EdgeEngine::new(self.dm.divide());

        let rs = edge_engine
            .execute1(&ScriptTree {
                script: [
                    "$->$output = = root->name _",
                    "$->$output += = root->ip _",
                    "$->$output += = root->port _",
                ]
                .join("\n"),
                name: "info".to_string(),
                next_v: vec![],
            })
            .await?;
        log::debug!("{rs}");
        let name = rs["info"][0].as_str().unwrap();
        let ip = rs["info"][1].as_str().unwrap();
        let port = rs["info"][2].as_str().unwrap();

        // build our application with a route
        let app = Router::new()
            .route(&format!("/{}/register", name), routing::post(http_register))
            .route(&format!("/{}/login", name), routing::post(http_login))
            .route(
                &format!("/{}/parse_token", name),
                routing::post(http_parse_token),
            )
            .route(&format!("/{}/execute", name), routing::post(http_execute))
            .route(&format!("/{}/execute1", name), routing::post(http_execute1))
            .with_state(Arc::new(self.dm));
        // run our app with hyper, listening globally on port 3000
        let address = format!("{}:{}", ip, port);
        log::info!("serving at {address}/{}", name);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await
    }
}
