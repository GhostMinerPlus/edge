mod graph;

use std::io::{self, Error, ErrorKind};

use sqlx::MySqlConnection;

pub use graph::{get_target, insert_edge, new_point};

pub async fn delete_edge(conn: &mut MySqlConnection, id: &str) -> io::Result<()> {
    log::info!("deleting edge:{id}");

    sqlx::query("delete from edge_t where id=?")
        .bind(id)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

#[async_recursion::async_recursion]
pub async fn set(
    conn: &mut MySqlConnection,
    root: &str,
    path: &str,
    value: &str,
) -> io::Result<String> {
    if path.is_empty() {
        return Ok(String::new());
    }

    log::debug!("set {value} {root}{path}");

    let arrow = &path[0..2];
    let path = &path[2..];

    let _v = path.find("->");
    let v_ = path.find("<-");
    if _v.is_some() || v_.is_some() {
        let pos = if _v.is_some() && v_.is_some() {
            std::cmp::min(_v.unwrap(), v_.unwrap())
        } else if _v.is_some() {
            _v.unwrap()
        } else {
            v_.unwrap()
        };
        let code = &path[0..pos];
        let path = &path[pos..];

        let pt = if arrow == "->" {
            graph::get_target_anyway(conn, root, code).await?
        } else {
            graph::get_source_anyway(conn, code, root).await?
        };
        set(conn, &pt, path, value).await
    } else {
        graph::set_target(conn, root, path, value).await
    }
}

pub async fn unwrap_value(
    conn: &mut MySqlConnection,
    root: &str,
    value: &str,
) -> io::Result<String> {
    if value.starts_with("->") || value.starts_with("<-") {
        graph::get(conn, root, value).await
    } else if value.starts_with("\"") {
        Ok(value[1..value.len() - 1].to_string())
    } else {
        Ok(value.to_string())
    }
}
