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
        .execute(
            conn.acquire()
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?,
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(id)
}

async fn __delete_edge(conn: &mut MySqlConnection, id: &str) -> io::Result<()> {
    log::info!("deleting edge:{id}");
    sqlx::query("delete from edge_t where id = ?")
        .bind(id)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

async fn __insert_edge_and_react(
    conn: &mut MySqlConnection,
    edge_form: &EdgeFrom,
) -> io::Result<String> {
    // Insert edge
    let id = __insert_edge(conn, edge_form).await?;
    // React
    match edge_form.code.as_str() {
        "deleted" => __delete_edge(conn, &edge_form.target).await?,
        _ => (),
    }
    Ok(id)
}

async fn insert_edge_v(
    conn: &mut MySqlConnection,
    edge_form_v: &Vec<EdgeFrom>,
) -> io::Result<Vec<String>> {
    let mut arr = Vec::new();
    for edge_form in edge_form_v {
        let id = __insert_edge_and_react(conn, edge_form).await?;
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
    use std::{fs, io};

    use sqlx::{Acquire, MySql, Pool};

    use crate::Config;

    use super::EdgeFrom;

    fn init() {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("INFO"))
            .is_test(true)
            .try_init();
    }

    #[test]
    fn insert_edge_v() {
        init();
        let f = async {
            let config: Config =
                toml::from_str(&fs::read_to_string("config.toml").unwrap()).unwrap();

            let pool: Pool<MySql> = sqlx::Pool::connect(&config.db_url).await.unwrap();
            let mut tr = pool.begin().await.unwrap();
            let conn = tr.acquire().await.unwrap();

            let r: io::Result<()> = (|| async move {
                let id_v = super::insert_edge_v(
                    conn,
                    &vec![EdgeFrom {
                        context: String::new(),
                        source: String::new(),
                        code: String::new(),
                        target: String::new(),
                    }],
                )
                .await?;
                super::insert_edge_v(
                    conn,
                    &vec![EdgeFrom {
                        context: String::new(),
                        source: String::new(),
                        code: "deleted".to_string(),
                        target: id_v[0].clone(),
                    }],
                )
                .await?;
                Ok(())
            })()
            .await;

            tr.rollback().await.unwrap();
            r.unwrap();
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f);
    }

    #[test]
    fn test_new_point() {
        init();
        let f = async {
            let id = super::new_point();
            let id1 = super::new_point();
            assert_ne!(id, id1);
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }
}
