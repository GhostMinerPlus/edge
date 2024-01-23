use std::io;

use sqlx::MySqlConnection;

use crate::util::graph::{self, get_list, get_target, get_target_v, new_point};

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

    if path.starts_with("->") || path.starts_with("<-") {
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
        log::debug!("set {value} {root}{path}");

        set(conn, root, path, value).await
    }
}

#[async_recursion::async_recursion]
pub async fn append(
    conn: &mut MySqlConnection,
    root: &str,
    path: &str,
    value: &str,
) -> io::Result<String> {
    if path.is_empty() {
        return Ok(String::new());
    }

    if path.starts_with("->") || path.starts_with("<-") {
        log::debug!("append {value} {root}{path}");
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
            append(conn, &pt, path, value).await
        } else {
            graph::append_target(conn, root, path, value).await
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
        log::debug!("append {value} {root}{path}");

        append(conn, root, path, value).await
    }
}

pub async fn dump(conn: &mut MySqlConnection, target: &str) -> io::Result<String> {
    let root = get_target(conn, target, "root").await?;
    let dimension_v = get_target_v(conn, target, "dimension").await?;
    let attr_v = get_target_v(conn, target, "attr").await?;

    let rs = get_list(conn, &root, &dimension_v, &attr_v).await?;
    Ok(json::stringify(rs))
}

pub async fn unwrap_value(
    conn: &mut MySqlConnection,
    root: &str,
    value: &str,
) -> io::Result<String> {
    if value == "?" {
        Ok(new_point())
    } else if value.starts_with("\"") {
        Ok(value[1..value.len() - 1].to_string())
    } else if value.contains("->") || value.contains("<-") {
        graph::get_or_empty(conn, root, value).await
    } else {
        Ok(value.to_string())
    }
}
