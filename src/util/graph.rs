mod raw {
    use std::io::{self, Error, ErrorKind};

    use sqlx::MySqlConnection;

    pub async fn delete_code(
        conn: &mut MySqlConnection,
        source: &str,
        code: &str,
    ) -> io::Result<()> {
        log::debug!("delete_code: {source}->{code}");

        sqlx::query("delete from edge_t where source=? and code=?")
            .bind(&source)
            .bind(&code)
            .execute(conn)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    #[async_recursion::async_recursion]
    pub async fn get(conn: &mut MySqlConnection, root: &str, path: &str) -> io::Result<String> {
        if path.starts_with("->") || path.starts_with("<-") {
            let (arrow, path) = (&path[0..2], &path[2..]);
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
                    super::get_target(conn, root, code).await?
                } else {
                    super::get_source(conn, code, root).await?
                };
                get(conn, &pt, path).await
            } else {
                if arrow == "->" {
                    super::get_target(conn, root, path).await
                } else {
                    super::get_source(conn, path, root).await
                }
            }
        } else {
            let _v = path.find("->");
            let v_ = path.find("<-");
            let pos = if _v.is_some() && v_.is_some() {
                std::cmp::min(_v.unwrap(), v_.unwrap())
            } else if _v.is_some() {
                _v.unwrap()
            } else {
                v_.unwrap()
            };
            let root = &path[0..pos];
            let path = &path[pos..];

            get(conn, root, path).await
        }
    }
}

use std::io::{self, Error, ErrorKind};

use sqlx::{MySqlConnection, Row};

// Public
pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub async fn get_or_empty(
    conn: &mut MySqlConnection,
    root: &str,
    path: &str,
) -> io::Result<String> {
    match raw::get(conn, root, path).await {
        Ok(r) => Ok(r),
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => Ok(String::new()),
            _ => Err(e),
        },
    }
}

pub async fn get_list(
    conn: &mut MySqlConnection,
    first: &mut String,
    attr_v: &Vec<String>,
) -> io::Result<json::Array> {
    let mut arr = json::Array::new();
    let item_v = attr_v
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
    let join_v = attr_v
        .into_iter()
        .map(|attr| format!("join edge_t {attr}_t on {attr}_t.source = t.target"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc}\n{item}")
            }
        })
        .unwrap();
    let condition = attr_v
        .into_iter()
        .map(|attr| format!("{attr}_t.code = '{attr}'"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc} and {item}")
            }
        })
        .unwrap();
    let sql = format!(
        "WITH RECURSIVE cte as (
SELECT *
FROM edge_t
WHERE source = '{first}' AND code = 'next'
UNION ALL
SELECT iter.*
FROM edge_t iter
JOIN cte ON iter.source = cte.target
WHERE iter.code = 'next'
)
SELECT {item_v}, t.target
FROM
(SELECT '{first}' as target
UNION ALL
SELECT target FROM cte) t
{join_v}
WHERE {condition}"
    );
    let rs = sqlx::query(&sql)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    for row in rs {
        let mut obj = json::object! {};
        for i in 0..attr_v.len() {
            let attr = &attr_v[i];
            obj[attr] = json::JsonValue::String(row.get(i));
        }
        *first = row.get(attr_v.len());
        arr.push(obj);
    }
    Ok(arr)
}

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
    let row = sqlx::query("select target from edge_t where source=? and code=?")
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

pub async fn get_target_or_empty(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<String> {
    let rs = get_target(conn, source, code).await;
    match rs {
        Ok(target) => Ok(target),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => Ok(String::new()),
            _ => Err(e),
        },
    }
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let row_v = sqlx::query("select target from edge_t where source=? and code=?")
        .bind(source)
        .bind(code)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut rs = Vec::new();
    for row in row_v {
        rs.push(row.get(0));
    }
    Ok(rs)
}

pub async fn get_target_anyway(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<String> {
    match get_target(conn, source, code).await {
        Ok(target) => Ok(target),
        Err(_) => {
            let target = new_point();
            insert_edge(conn, source, code, 0, &target).await?;
            Ok(target)
        }
    }
}

pub async fn get_source(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let row = sqlx::query("select source from edge_t where code=? and target=?")
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

pub async fn get_next_no(conn: &mut MySqlConnection, source: &str, code: &str) -> io::Result<u64> {
    let rs = sqlx::query(
        "select max(no) + 1 from edge_t group by source, code having source=? and code=?",
    )
    .bind(source)
    .bind(code)
    .fetch_all(conn)
    .await
    .map_err(|e| Error::new(ErrorKind::Other, e))?;
    if rs.is_empty() {
        return Ok(0);
    }
    Ok(rs[0].get(0))
}

pub async fn get_source_anyway(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<String> {
    match get_source(conn, code, target).await {
        Ok(source) => Ok(source),
        Err(_) => {
            let source = new_point();
            insert_edge(conn, &source, code, 0, target).await?;
            Ok(source)
        }
    }
}

pub async fn set_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    raw::delete_code(conn, source, code).await?;
    insert_edge(conn, source, code, 0, target).await
}

pub async fn append_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let no = get_next_no(conn, source, code).await?;
    insert_edge(conn, source, code, no, target).await
}
