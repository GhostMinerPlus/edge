use std::io::{self, Error, ErrorKind};

use edge::Edge;
use serde::Deserialize;
use sqlx::{MySqlConnection, Row};

#[derive(Deserialize)]
pub struct EdgeFrom {
    context: String,
    source: String,
    code: String,
    target: String,
}

async fn insert_edge(conn: &mut MySqlConnection, edge_form: &EdgeFrom) -> io::Result<Edge> {
    // new edge
    let edge = Edge {
        id: new_point(),
        context: edge_form.context.clone(),
        source: edge_form.source.clone(),
        code: edge_form.code.clone(),
        target: edge_form.target.clone(),
    };
    // insert
    sqlx::query("insert into edge_t (id,context,source,code,target) values (?,?,?,?,?)")
        .bind(&edge.id)
        .bind(&edge.context)
        .bind(&edge.source)
        .bind(&edge.code)
        .bind(&edge.target)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(edge)
}

async fn get_target_by_code(
    conn: &mut MySqlConnection,
    context: &str,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where context=? and source=? and code=?")
        .bind(context)
        .bind(source)
        .bind(code)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0))
    }
    Ok(arr)
}

async fn delete_edge(conn: &mut MySqlConnection, id: &str) -> io::Result<()> {
    log::info!("deleting edge:{id}");

    sqlx::query("delete from edge_t where id=?")
        .bind(id)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

#[async_recursion::async_recursion]
async fn act(conn: &mut MySqlConnection, edge: &Edge, action_v: &Vec<String>) -> io::Result<()> {
    for action in action_v {
        let code_v = get_target_by_code(conn, &edge.context, action, "code").await?;
        if code_v.len() != 1 {
            return Err(Error::new(ErrorKind::Other, ""));
        }
        match code_v[0].as_str() {
            "deleted" => delete_edge(conn, &edge.target).await?,
            _ => (),
        }
        let next_action_v = get_target_by_code(conn, &edge.context, action, "next").await?;
        act(conn, edge, &next_action_v).await?;
    }

    Ok(())
}

#[async_recursion::async_recursion]
async fn insert_edge_and_act(conn: &mut MySqlConnection, edge_form: &EdgeFrom) -> io::Result<Edge> {
    // Insert edge
    let edge = insert_edge(conn, edge_form).await?;
    // Act
    let action_v = get_target_by_code(conn, &edge.context, &edge.code, "action").await?;
    act(conn, &edge, &action_v).await?;
    Ok(edge)
}

pub async fn insert_edge_v(
    conn: &mut MySqlConnection,
    edge_form_v: &Vec<EdgeFrom>,
) -> io::Result<Vec<Edge>> {
    let mut arr = Vec::new();
    for edge_form in edge_form_v {
        let edge = insert_edge_and_act(conn, edge_form).await?;
        arr.push(edge);
    }
    Ok(arr)
}

pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
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
                let edge_v = super::insert_edge_v(
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
                        target: edge_v[0].id.clone(),
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
