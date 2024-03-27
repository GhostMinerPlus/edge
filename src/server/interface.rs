use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use sqlx::Acquire;

use crate::{
    err::{Error, ErrorKind},
    server::context::Context,
};

use super::edge_service;

// Public
#[derive(Deserialize)]
pub struct Require {
    target: String,
    constraint: String,
}

pub async fn http_execute(
    State(state): State<Arc<Context>>,
    script_vn: String,
) -> (StatusCode, String) {
    match (|| async {
        let mut tr = state
            .pool
            .begin()
            .await
            .map_err(|e| Error::new(ErrorKind::BadConnection, e.to_string()))?;
        let conn = tr
            .acquire()
            .await
            .map_err(|e| Error::new(ErrorKind::BadConnection, e.to_string()))?;
        let mut mem_table = state.mem_table.lock().await;
        // Execute
        let r = match edge_service::execute(conn, &mut mem_table, &json::parse(&script_vn).unwrap())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = tr.rollback().await;
                return Err(e);
            }
        };
        // commit
        if let Err(e) = tr.commit().await {
            return Err(Error::new(ErrorKind::BadConnection, e.to_string()));
        }
        // json
        Ok(r)
    })()
    .await
    {
        Ok(r) => (StatusCode::OK, json::stringify(r)),
        Err(e) => {
            log::error!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

pub async fn http_require(
    State(state): State<Arc<Context>>,
    Json(require): Json<Require>,
) -> (StatusCode, String) {
    match (|| async {
        let mut tr = state
            .pool
            .begin()
            .await
            .map_err(|e| Error::new(ErrorKind::BadConnection, e.to_string()))?;
        let conn = tr
            .acquire()
            .await
            .map_err(|e| Error::new(ErrorKind::BadConnection, e.to_string()))?;
        let mut mem_table = state.mem_table.lock().await;
        // Execute
        let r =
            match edge_service::require(conn, &mut mem_table, &require.target, &require.constraint)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = tr.rollback().await;
                    return Err(e);
                }
            };
        // commit
        if let Err(e) = tr.commit().await {
            return Err(Error::new(ErrorKind::BadConnection, e.to_string()));
        }
        // json
        Ok(r)
    })()
    .await
    {
        Ok(r) => (StatusCode::OK, json::stringify(r)),
        Err(e) => {
            log::error!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}
