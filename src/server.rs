//! Server that provides services.
mod crypto;
mod service;

use std::{collections::HashMap, io, sync::Arc};

use axum::{http::HeaderMap, routing, Router};
use edge_lib::{data::AsDataManager, util::Path, EdgeEngine, ScriptTree};

use crate::err;

pub struct HttpServer {
    dm: Arc<dyn AsDataManager>,
}

impl HttpServer {
    pub fn new(dm: Arc<dyn AsDataManager>) -> Self {
        Self { dm }
    }

    pub async fn run(self) -> io::Result<()> {
        let mut edge_engine = EdgeEngine::new(self.dm.clone());

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
            .route(
                &format!("/{}/register", name),
                routing::post(main::http_register),
            )
            .route(&format!("/{}/login", name), routing::post(main::http_login))
            .route(
                &format!("/{}/parse_token", name),
                routing::post(main::http_parse_token),
            )
            .route(
                &format!("/{}/execute", name),
                routing::post(main::http_execute),
            )
            .route(
                &format!("/{}/execute1", name),
                routing::post(main::http_execute1),
            )
            .with_state(self.dm);
        // run our app with hyper, listening globally on port 3000
        let address = format!("{}:{}", ip, port);
        log::info!("serving at {address}/{}", name);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await
    }
}

fn get_cookie(hm: &HeaderMap) -> err::Result<HashMap<String, String>> {
    let cookie: &str = match hm.get("Cookie") {
        Some(r) => match r.to_str() {
            Ok(r) => r,
            Err(e) => {
                return Err(err::Error::Other(e.to_string()));
            }
        },
        None => {
            return Err(err::Error::Other(format!("no cookie")));
        }
    };
    let pair_v: Vec<Vec<&str>> = cookie
        .split(';')
        .into_iter()
        .map(|pair| pair.split('=').collect::<Vec<&str>>())
        .collect();
    let mut cookie = HashMap::with_capacity(pair_v.len());
    for pair in pair_v {
        if pair.len() != 2 {
            continue;
        }
        cookie.insert(pair[0].to_string(), pair[1].to_string());
    }
    Ok(cookie)
}

async fn parse_token(dm: Arc<dyn AsDataManager>, token: &str) -> err::Result<String> {
    let key = dm
        .get(&Path::from_str("root->key"))
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    if key.is_empty() {
        return Err(err::Error::Other("no key".to_string()));
    }
    crypto::parse_token(&key[0], token)
}

async fn parse_auth(
    dm: Arc<dyn AsDataManager>,
    cookie: &HashMap<String, String>,
) -> err::Result<(String, String)> {
    let writer_token = match cookie.get("writer") {
        Some(r) => r,
        None => {
            return Err(err::Error::Other("no token".to_lowercase()));
        }
    };
    let writer = parse_token(dm.clone(), writer_token).await?;
    let printer = match cookie.get("printer") {
        Some(app) => parse_token(dm, app).await?,
        None => writer.clone(),
    };
    Ok((writer, printer))
}

mod main {
    use std::sync::Arc;

    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        response::Response,
        Json,
    };
    use edge_lib::data::AsDataManager;

    use crate::err;

    use super::{crypto, get_cookie, parse_auth, service};

    pub async fn http_register(
        State(dm): State<Arc<dyn AsDataManager>>,
        Json(auth): Json<crypto::Auth>,
    ) -> (StatusCode, String) {
        match service::register(dm, &auth).await {
            Ok(_) => (StatusCode::OK, format!("success")),
            Err(e) => {
                log::warn!("when http_register:\n{e}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    pub async fn http_login(
        State(dm): State<Arc<dyn AsDataManager>>,
        Json(auth): Json<crypto::Auth>,
    ) -> Response<String> {
        match service::login(dm, &auth).await {
            Ok(token) => Response::builder()
                .header("Set-Cookie", format!("token={token}; Path=/"))
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

    pub async fn http_parse_token(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
    ) -> (StatusCode, String) {
        let cookie = match get_cookie(&hm) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("when http_parse_token:\n{e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
        };
        match parse_auth(dm, &cookie).await {
            Ok(s) => (StatusCode::OK, serde_json::to_string(&s).unwrap()),
            Err(e) => {
                log::warn!("when http_parse_token:\n{e}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    pub async fn http_execute(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        script_vn: String,
    ) -> Response<String> {
        match service::execute(dm, &hm, script_vn).await {
            Ok(s) => Response::builder().status(StatusCode::OK).body(s).unwrap(),
            Err(e) => {
                log::warn!("when http_execute:\n{e}");
                match e {
                    err::Error::Other(msg) => Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(msg)
                        .unwrap(),
                    err::Error::NotLogin(msg) => Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(msg)
                        .unwrap(),
                }
            }
        }
    }

    pub async fn http_execute1(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        script_vn: String,
    ) -> Response<String> {
        match service::execute1(dm, &hm, script_vn).await {
            Ok(s) => Response::builder().status(StatusCode::OK).body(s).unwrap(),
            Err(e) => {
                log::warn!("when http_execute:\n{e}");
                match e {
                    err::Error::Other(msg) => Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(msg)
                        .unwrap(),
                    err::Error::NotLogin(msg) => Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(msg)
                        .unwrap(),
                }
            }
        }
    }
}
