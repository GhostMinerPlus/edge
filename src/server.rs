//! Server that provides services.
mod crypto;
mod service;

use std::{io, sync::Arc};

use axum::{routing, Router};
use edge_lib::{data::AsDataManager, EdgeEngine, ScriptTree};
use serde::Deserialize;

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
                routing::post(main::post_register),
            )
            .route(&format!("/{}/login", name), routing::post(main::post_login))
            .route(
                &format!("/{}/parse_token", name),
                routing::post(main::post_parse_token),
            )
            .route(
                &format!("/{}/execute", name),
                routing::post(main::post_execute),
            )
            .route(
                &format!("/{}/execute1", name),
                routing::post(main::post_execute1),
            )
            .route(&format!("/{}/paper", name), routing::put(main::put_paper))
            .route(
                &format!("/{}/paper", name),
                routing::delete(main::delete_paper),
            )
            .route(&format!("/{}/paper", name), routing::get(main::get_paper))
            .route(&format!("/{}/paper", name), routing::post(main::post_paper))
            .route(
                &format!("/{}/paper/writer", name),
                routing::post(main::get_paper_writer),
            )
            .with_state(self.dm);
        // run our app with hyper, listening globally on port 3000
        let address = format!("{}:{}", ip, port);
        log::info!("serving at {address}/{}", name);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await
    }
}

#[derive(Deserialize)]
struct Paper {
    paper_id: String,
    name: String,
    writer_v: Vec<String>,
    manager_v: Vec<String>,
}

mod main {
    use std::{collections::HashMap, sync::Arc};

    use axum::{
        extract::{Query, State},
        http::{HeaderMap, Response, StatusCode},
        Json,
    };
    use edge_lib::{data::AsDataManager, util::Path, ScriptTree};
    use serde::Deserialize;

    use crate::err;

    use super::{crypto, service, Paper};

    pub async fn post_register(
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

    pub async fn post_login(
        State(dm): State<Arc<dyn AsDataManager>>,
        Json(auth): Json<crypto::Auth>,
    ) -> Response<String> {
        match service::login(dm, &auth).await {
            Ok(token) => Response::builder()
                .header("Set-Cookie", format!("writer={token}; Path=/"))
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

    pub async fn post_parse_token(
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

    pub async fn post_execute(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        body: String,
    ) -> Response<String> {
        let (writer, printer) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("when post_execute:\n{e}");
                return map_err(e);
            }
        };
        let body_json = json::parse(&body).unwrap();
        let paper = body_json["paper"].as_str().unwrap();
        let script_vn = &body_json["script"];
        match service::execute(dm, writer, paper.to_string(), printer, script_vn).await {
            Ok(s) => Response::builder().status(StatusCode::OK).body(s).unwrap(),
            Err(e) => {
                log::warn!("when post_execute:\n{e}");
                map_err(e)
            }
        }
    }

    #[derive(Deserialize)]
    pub struct ScriptWithPaper {
        paper: String,
        script: ScriptTree,
    }

    pub async fn post_execute1(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        body: String,
    ) -> Response<String> {
        let (writer, printer) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("when post_execute1:\n{e}");
                return map_err(e);
            }
        };
        let swp: ScriptWithPaper = serde_json::from_str(&body).unwrap();
        match service::execute1(dm, writer, swp.paper, printer, &swp.script).await {
            Ok(s) => Response::builder().status(StatusCode::OK).body(s).unwrap(),
            Err(e) => {
                log::warn!("when post_execute1:\n{e}");
                map_err(e)
            }
        }
    }

    pub async fn put_paper(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        Json(paper): Json<Paper>,
    ) -> Response<String> {
        let (writer, _) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("{e}\nwhen put_paper");
                return map_err(e);
            }
        };
        match service::put_paper(dm, writer, paper).await {
            Ok(s) => Response::builder().status(StatusCode::OK).body(s).unwrap(),
            Err(e) => {
                log::warn!("{e}\nwhen put_paper");
                map_err(e)
            }
        }
    }

    #[derive(Deserialize)]
    pub struct PaperQuery {
        paper_id: String,
    }

    pub async fn delete_paper(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        Query(paper): Query<PaperQuery>,
    ) -> Response<String> {
        let (writer, _) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("{e}\nwhen delete_paper");
                return map_err(e);
            }
        };
        match service::delete_paper(dm, writer, paper.paper_id).await {
            Ok(_) => Response::builder()
                .status(StatusCode::OK)
                .body("success".to_string())
                .unwrap(),
            Err(e) => {
                log::warn!("{e}\nwhen delete_paper");
                map_err(e)
            }
        }
    }

    pub async fn get_paper(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
    ) -> Response<String> {
        let (writer, _) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("{e}\nwhen get_paper");
                return map_err(e);
            }
        };
        match service::get_paper(dm, writer).await {
            Ok(s) => Response::builder()
                .status(StatusCode::OK)
                .body(s)
                .unwrap(),
            Err(e) => {
                log::warn!("{e}\nwhen get_paper");
                map_err(e)
            }
        }
    }

    pub async fn get_paper_writer(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        Query(paper): Query<PaperQuery>,
    ) -> Response<String> {
        let (writer, _) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("{e}\nwhen delete_paper");
                return map_err(e);
            }
        };
        match service::get_paper_writer(dm, writer, paper.paper_id).await {
            Ok(s) => Response::builder()
                .status(StatusCode::OK)
                .body(s)
                .unwrap(),
            Err(e) => {
                log::warn!("{e}\nwhen get_paper");
                map_err(e)
            }
        }
    }

    pub async fn post_paper(
        hm: HeaderMap,
        State(dm): State<Arc<dyn AsDataManager>>,
        Json(paper): Json<Paper>,
    ) -> Response<String> {
        let (writer, _) = match parse_auth_by_header(dm.clone(), &hm).await {
            Ok(rs) => rs,
            Err(e) => {
                log::warn!("{e}\nwhen delete_paper");
                return map_err(e);
            }
        };
        match service::update_paper(dm, writer, paper).await {
            Ok(_) => Response::builder()
                .status(StatusCode::OK)
                .body("success".to_string())
                .unwrap(),
            Err(e) => {
                log::warn!("{e}\nwhen get_paper");
                map_err(e)
            }
        }
    }

    async fn parse_auth_by_header(
        dm: Arc<dyn AsDataManager>,
        hm: &HeaderMap,
    ) -> err::Result<(String, String)> {
        let cookie = get_cookie(hm).map_err(|e| err::Error::NotLogin(e.to_string()))?;
        let (writer, printer) = parse_auth(dm.clone(), &cookie)
            .await
            .map_err(|e| err::Error::NotLogin(e.to_string()))?;
        log::info!("email: {}", writer);
        Ok((writer, printer))
    }

    fn map_err(e: err::Error) -> Response<String> {
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
}
