mod raw {
    use std::io;

    use sqlx::MySqlConnection;

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
