use std::io;

use sqlx::MySqlConnection;

use crate::app::new_point;

mod cache;

#[async_recursion::async_recursion]
async fn get(conn: &mut MySqlConnection, root: &str, path: &str) -> io::Result<String> {
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
                get_target(conn, root, code).await?
            } else {
                get_source(conn, code, root).await?
            };
            get(conn, &pt, path).await
        } else {
            if arrow == "->" {
                get_target(conn, root, path).await
            } else {
                get_source(conn, path, root).await
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

// Public
pub use cache::{
    append_target, get_list, get_source, get_target, get_target_v, insert_edge, set_target,
};

pub async fn get_or_empty(
    conn: &mut MySqlConnection,
    root: &str,
    path: &str,
) -> io::Result<String> {
    match get(conn, root, path).await {
        Ok(r) => Ok(r),
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => Ok(String::new()),
            _ => Err(e),
        },
    }
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
