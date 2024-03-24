mod edge_service;

use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use axum::{extract::State, http::StatusCode, Json};
use sqlx::Acquire;

use crate::app::AppState;

pub async fn http_execute(
    State(state): State<Arc<AppState>>,
    Json(script_v): Json<Vec<String>>,
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
        let mut mem_table = state.mem_table.lock().await;
        // Execute
        let r = match edge_service::execute(conn, &mut mem_table, &script_v).await {
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
        Ok(r)
    })()
    .await
    {
        Ok(r) => (StatusCode::OK, json::stringify(r)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
