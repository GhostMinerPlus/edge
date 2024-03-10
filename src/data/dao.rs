use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use sqlx::{MySqlConnection, Row};

use crate::mem_table::Edge;

// Public
pub async fn delete_edge_with_source_code(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<()> {
    sqlx::query("delete from edge_t where source = ? and code = ?")
        .bind(source)
        .bind(code)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn insert_edge_mp(
    conn: &mut MySqlConnection,
    edge_mp: &HashMap<String, Edge>,
) -> io::Result<()> {
    if edge_mp.is_empty() {
        return Ok(());
    }
    log::info!("commit edge_mp: {}", edge_mp.len());
    let value_v = edge_mp
        .into_iter()
        .map(|_| format!("(?,?,?,?,?)"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let sql = format!("insert into edge_t (id,source,code,target) values {value_v}");
    let mut statement = sqlx::query(&sql);
    for (_, edge) in edge_mp {
        statement = statement
            .bind(&edge.id)
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

pub async fn get_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<(String, String)> {
    let row =
        sqlx::query("select id, target from edge_t where source=? and code=?  order by id limit 1")
            .bind(source)
            .bind(code)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    Ok((row.get(0), row.get(1)))
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where source=? and code=? order by id")
        .bind(source)
        .bind(code)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
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
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select source from edge_t where code=? and target=? order by id")
        .bind(code)
        .bind(target)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

pub async fn get_source(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<(String, String)> {
    let row =
        sqlx::query("select id, source from edge_t where code=? and target=?  order by id limit 1")
            .bind(code)
            .bind(target)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    Ok((row.get(0), row.get(1)))
}

pub async fn get_list(
    conn: &mut MySqlConnection,
    root: &str,
    dimension_v: &Vec<String>,
    attr_v: &Vec<String>,
) -> io::Result<json::Array> {
    let mut arr = json::Array::new();
    let dimension_item_v = dimension_v
        .into_iter()
        .map(|dimension| format!("{dimension}_t.target as {dimension}"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let attr_item_v = attr_v
        .into_iter()
        .map(|attr| format!("{attr}_t.target as {attr}"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let mut pre_dimension = String::new();
    let dimension_join_v = dimension_v
        .into_iter()
        .enumerate()
        .map(|(i, dimension)| {
            if i == 0 {
                pre_dimension = dimension.clone();
                format!("(select * from edge_t where code = '{dimension}' and source = '{root}') {dimension}_t")
            } else {
                let r = format!("(select * from edge_t where code = '{dimension}') {dimension}_t on {dimension}_t.source = {pre_dimension}_t.target");
                pre_dimension = dimension.clone();
                return r;
            }
        })
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc}\njoin {item}")
            }
        })
        .unwrap();
    let last_dimension = dimension_v.last().unwrap().clone();
    let attr_join_v = attr_v
        .into_iter()
        .map(|attr| format!("(select * from edge_t where code = '{attr}') {attr}_t on {attr}_t.source = {last_dimension}_t.target"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc}\njoin {item}")
            }
        })
        .unwrap();
    let sql = format!(
        "SELECT {dimension_item_v}, {attr_item_v}
FROM {dimension_join_v}
join {attr_join_v}
order by {last_dimension}_t.id"
    );
    log::debug!("{sql}");
    let rs = sqlx::query(&sql)
        .bind(root)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    for row in rs {
        let mut obj = json::object! {};
        for i in 0..dimension_v.len() {
            let attr = &dimension_v[i];
            obj[attr] = json::JsonValue::String(row.get(i));
        }
        for i in dimension_v.len()..dimension_v.len() + attr_v.len() {
            let attr = &attr_v[i - dimension_v.len()];
            obj[attr] = json::JsonValue::String(row.get(i));
        }
        arr.push(obj);
    }
    Ok(arr)
}

pub async fn delete(conn: &mut MySqlConnection, point: &str) -> io::Result<()> {
    sqlx::query("DELETE FROM edge_t WHERE source = ? OR code = ? OR target = ?")
        .bind(point)
        .bind(point)
        .bind(point)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_code(conn: &mut MySqlConnection, code: &str) -> io::Result<()> {
    sqlx::query("delete from edge_t where code = ?")
        .bind(code)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_code_without_source(
    conn: &mut MySqlConnection,
    code: &str,
    source_code: &str,
) -> io::Result<()> {
    sqlx::query(
        "delete from edge_t
where code = ?
and not exists (
select 1 from edge_t v where v.code = ? and v.target = source
)",
    )
    .bind(code)
    .bind(source_code)
    .execute(conn)
    .await
    .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_code_without_target(
    conn: &mut MySqlConnection,
    code: &str,
    target_code: &str,
) -> io::Result<()> {
    sqlx::query(
        "delete from edge_t
where code = ?
and not exists (
select 1 from edge_t v where v.code = ? and v.source = target
)",
    )
    .bind(code)
    .bind(target_code)
    .execute(conn)
    .await
    .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}
