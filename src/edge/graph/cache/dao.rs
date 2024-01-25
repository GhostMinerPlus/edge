use std::io::{self, Error, ErrorKind};

use sqlx::{MySqlConnection, Row};

use crate::mem_table::new_point;

// Public
pub async fn insert_edge(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    no: u64,
    target: &str,
) -> io::Result<String> {
    log::debug!("insert_edge: {source}->{code}={target}");

    let id = new_point();
    sqlx::query("insert into edge_t (id,source,code,no,target) values (?,?,?,?,?)")
        .bind(&id)
        .bind(&source)
        .bind(&code)
        .bind(no)
        .bind(&target)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(id)
}

pub async fn get_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<String> {
    let row =
        sqlx::query("select target from edge_t where source=? and code=? order by no desc limit 1")
            .bind(source)
            .bind(code)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    let target = row.get(0);
    Ok(target)
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where source=? and code=? order by no")
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

pub async fn get_source(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let row =
        sqlx::query("select source from edge_t where code=? and target=? order by no desc limit 1")
            .bind(code)
            .bind(target)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    Ok(row.get(0))
}

pub async fn set_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let id = new_point();
    let sql = format!(
        "insert into edge_t (id, source, code, no, target)
values
('{id}', '{source}', '{code}', ifnull((select max(t.no) from edge_t t where t.source = '{source}' and t.code = '{code}'), 0), '{target}')
on duplicate key update
target = '{target}'"
    );
    sqlx::query(&sql)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(id)
}

pub async fn append_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let id = new_point();
    let sql = format!(
        "insert into edge_t (id, source, code, no, target)
values
('{id}', '{source}', '{code}', ifnull((select max(t.no) + 1 from edge_t t where t.source = '{source}' and t.code = '{code}'), 0), '{target}')"
    );
    sqlx::query(&sql)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(id)
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
    let order_v = dimension_v
        .into_iter()
        .map(|dimension| format!("{dimension}_t.no"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let sql = format!(
        "SELECT {dimension_item_v}, {attr_item_v}
FROM {dimension_join_v}
join {attr_join_v}
order by {order_v}"
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
