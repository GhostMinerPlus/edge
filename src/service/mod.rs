mod edge_service;

use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use axum::{extract::State, http::StatusCode, Json};
use sqlx::Acquire;

use crate::AppState;

use self::edge_service::Inc;

pub async fn http_new_point() -> (StatusCode, String) {
    (StatusCode::OK, edge_service::new_point())
}

pub async fn http_execute(
    State(state): State<Arc<AppState>>,
    Json(inc_v): Json<Vec<Inc>>,
) -> (StatusCode, String) {
    match (|| async {
        let mut tr = state
            .pool
            .begin()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let conn = tr
            .acquire()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        // Execute
        let r = match edge_service::execute(conn, &inc_v).await {
            Ok(r) => r,
            Err(e) => {
                let _ = tr.rollback().await;
                log::error!("{e}");
                return Err(e);
            }
        };
        // commit
        if let Err(e) = tr.commit().await {
            log::error!("{e}");
            return Err(Error::new(ErrorKind::Other, e.to_string()));
        }
        // json
        Ok(json::from(r).dump())
    })()
    .await
    {
        Ok(r) => (StatusCode::OK, r),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
