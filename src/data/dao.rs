use std::collections::BTreeMap;

use sqlx::{MySqlConnection, Row};

use crate::{
    data::mem_table::Edge,
    err::{Error, ErrorKind, Result},
};

// Public
pub async fn delete_edge_with_source_code(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> Result<()> {
    sqlx::query("delete from edge_t where source = ? and code = ?")
        .bind(source)
        .bind(code)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn delete_edge_with_code_target(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> Result<()> {
    sqlx::query("delete from edge_t where code = ? and target = ?")
        .bind(code)
        .bind(target)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn insert_edge_mp(
    conn: &mut MySqlConnection,
    edge_mp: &BTreeMap<u64, Edge>,
) -> Result<()> {
    if edge_mp.is_empty() {
        return Ok(());
    }
    log::info!("commit edge_mp: {}", edge_mp.len());
    let value_v = edge_mp
        .into_iter()
        .map(|_| format!("(?,?,?)"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let sql = format!("insert into edge_t (source,code,target) values {value_v}");
    let mut statement = sqlx::query(&sql);
    for (_, edge) in edge_mp {
        statement = statement
            .bind(&edge.source)
            .bind(&edge.code)
            .bind(&edge.target);
    }

    statement
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn get_target(conn: &mut MySqlConnection, source: &str, code: &str) -> Result<String> {
    let row =
        sqlx::query("select target from edge_t where source=? and code=?  order by id limit 1")
            .bind(source)
            .bind(code)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::Other, e.to_string()),
                _ => Error::new(ErrorKind::Other, e.to_string()),
            })?;
    Ok(row.get(0))
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where source=? and code=? order by id")
        .bind(source)
        .bind(code)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

pub async fn get_source_v(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> Result<Vec<String>> {
    let rs = sqlx::query("select source from edge_t where code=? and target=? order by id")
        .bind(code)
        .bind(target)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

pub async fn get_source(conn: &mut MySqlConnection, code: &str, target: &str) -> Result<String> {
    let row =
        sqlx::query("select source from edge_t where code=? and target=?  order by id limit 1")
            .bind(code)
            .bind(target)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::Other, e.to_string()),
                _ => Error::new(ErrorKind::Other, e.to_string()),
            })?;
    Ok(row.get(0))
}
