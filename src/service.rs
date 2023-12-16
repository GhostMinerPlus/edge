use std::{
    io::{self, Error, ErrorKind},
    sync::Arc,
};

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use sqlx::{Acquire, MySqlConnection};

use crate::AppState;

#[derive(Deserialize)]
pub struct EdgeFrom {
    context: String,
    source: String,
    code: String,
    target: String,
}

async fn __insert_edge(conn: &mut MySqlConnection, edge_form: &EdgeFrom) -> io::Result<String> {
    let id = new_point();
    sqlx::query("insert into edge_t (id,context,source,code,target) values (?,?,?,?,?)")
        .bind(&id)
        .bind(&edge_form.context)
        .bind(&edge_form.source)
        .bind(&edge_form.code)
        .bind(&edge_form.target)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(id)
}

async fn insert_edge_v(
    conn: &mut MySqlConnection,
    edge_form_v: &Vec<EdgeFrom>,
) -> io::Result<Vec<String>> {
    let mut arr = Vec::new();
    for edge_form in edge_form_v {
        let id = __insert_edge(conn, edge_form).await?;
        arr.push(id);
    }
    Ok(arr)
}

fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub async fn http_insert_edge_v(
    State(state): State<Arc<AppState>>,
    Json(edge_form_v): Json<Vec<EdgeFrom>>,
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
        let id_v = match insert_edge_v(conn, &edge_form_v).await {
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
    (StatusCode::OK, new_point())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sqlx::{Acquire, MySql, Pool};

    use crate::Config;

    use super::EdgeFrom;

    #[test]
    fn insert_edge_v() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let config: Config =
                    toml::from_str(&fs::read_to_string("config.toml").unwrap()).unwrap();

                let pool: Pool<MySql> = sqlx::Pool::connect(&config.db_url).await.unwrap();
                let mut tr = pool.begin().await.unwrap();
                let conn = tr.acquire().await.unwrap();
                let r = super::insert_edge_v(
                    conn,
                    &vec![EdgeFrom {
                        context: String::new(),
                        source: String::new(),
                        code: String::new(),
                        target: String::new(),
                    }],
                )
                .await;
                tr.rollback().await.unwrap();
                r.unwrap();
            });
    }

    #[test]
    fn test_new_point() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let id = super::new_point();
                let id1 = super::new_point();
                assert_ne!(id, id1);
            })
    }
}
