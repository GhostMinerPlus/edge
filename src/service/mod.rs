mod edge;

use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use axum::{extract::State, http::StatusCode, Json};
use sqlx::Acquire;

use crate::AppState;

pub async fn http_insert_edge_v(
    State(state): State<Arc<AppState>>,
    Json(edge_form_v): Json<Vec<edge::EdgeFrom>>,
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
        let id_v = match edge::insert_edge_v(conn, &edge_form_v).await {
            Ok(r) => r,
            Err(e) => {
                let _ = tr.rollback().await;
                log::error!("{e}");
                return Err(e);
            }
        };
        if let Err(e) = tr.commit().await {
            log::error!("{e}");
            return Err(Error::new(ErrorKind::Other, e.to_string()));
        }
        serde_json::to_string(&id_v).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
    })()
    .await
    {
        Ok(r) => (StatusCode::OK, r),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

pub async fn http_new_point() -> (StatusCode, String) {
    (StatusCode::OK, edge::new_point())
}
