use std::{
    collections::BTreeMap,
    io::{self, Error, ErrorKind},
};

use edge_lib::mem_table::Edge;
use sqlx::{MySql, Pool, Row};

// Public
pub async fn delete_edge_with_source_code(
    pool: Pool<MySql>,
    source: &str,
    code: &str,
) -> io::Result<()> {
    sqlx::query("delete from edge_t where source = ? and code = ?")
        .bind(source)
        .bind(code)
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_edge_with_code_target(
    pool: Pool<MySql>,
    code: &str,
    target: &str,
) -> io::Result<()> {
    sqlx::query("delete from edge_t where code = ? and target = ?")
        .bind(code)
        .bind(target)
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn insert_edge_mp(
    pool: Pool<MySql>,
    edge_mp: &BTreeMap<u64, Edge>,
) -> io::Result<()> {
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
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn get_target(
    pool: Pool<MySql>,
    source: &str,
    code: &str,
) -> io::Result<String> {
    let row =
        sqlx::query("select target from edge_t where source=? and code=?  order by id limit 1")
            .bind(source)
            .bind(code)
            .fetch_one(&pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    Ok(row.get(0))
}

pub async fn get_target_v(
    pool: Pool<MySql>,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where source=? and code=? order by id")
        .bind(source)
        .bind(code)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

pub async fn get_source_v(
    pool: Pool<MySql>,
    code: &str,
    target: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select source from edge_t where code=? and target=? order by id")
        .bind(code)
        .bind(target)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

pub async fn get_source(
    pool: Pool<MySql>,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let row =
        sqlx::query("select source from edge_t where code=? and target=?  order by id limit 1")
            .bind(code)
            .bind(target)
            .fetch_one(&pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    Ok(row.get(0))
}
