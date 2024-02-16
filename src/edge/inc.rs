use std::io;

use crate::{
    data::AsDataManager,
    edge::graph::{get_or_empty, get_source_anyway, get_target_anyway},
    mem_table::new_point,
};

// Public
#[async_recursion::async_recursion]
pub async fn set(
    dm: &mut impl AsDataManager,
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
                get_target_anyway(dm, root, code).await?
            } else {
                get_source_anyway(dm, code, root).await?
            };
            set(dm, &pt, path, value).await
        } else {
            dm.set_target(root, path, value).await
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

        set(dm, root, path, value).await
    }
}

#[async_recursion::async_recursion]
pub async fn asign(
    dm: &mut impl AsDataManager,
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
                get_target_anyway(dm, root, code).await?
            } else {
                get_source_anyway(dm, code, root).await?
            };
            asign(dm, &pt, path, value).await
        } else {
            dm.insert_edge(root, path, 0, value).await
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

        asign(dm, root, path, value).await
    }
}

#[async_recursion::async_recursion]
pub async fn append(
    dm: &mut impl AsDataManager,
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
                get_target_anyway(dm, root, code).await?
            } else {
                get_source_anyway(dm, code, root).await?
            };
            append(dm, &pt, path, value).await
        } else {
            dm.append_target(root, path, value).await
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

        append(dm, root, path, value).await
    }
}

pub async fn dump(dm: &mut impl AsDataManager, target: &str) -> io::Result<String> {
    let root = dm.get_target(target, "$root").await?;
    let dimension_v = dm.get_target_v(target, "$dimension").await?;
    let attr_v = dm.get_target_v(target, "$attr").await?;

    let rs = dm.get_list(&root, &dimension_v, &attr_v).await?;
    Ok(json::stringify(rs))
}

pub async fn unwrap_value(
    dm: &mut impl AsDataManager,
    root: &str,
    value: &str,
) -> io::Result<String> {
    log::debug!("{value}");
    if value == "?" {
        Ok(new_point())
    } else if value.starts_with("\"") {
        Ok(value[1..value.len() - 1].to_string())
    } else if value.contains("->") || value.contains("<-") {
        get_or_empty(dm, root, value).await
    } else {
        Ok(value.to_string())
    }
}

pub async fn delete(dm: &mut impl AsDataManager, point: &str) -> io::Result<()> {
    dm.delete(point).await
}

pub async fn delete_code(dm: &mut impl AsDataManager, code: &str) -> io::Result<()> {
    dm.delete_code(code).await
}

pub async fn delete_code_without_source(
    dm: &mut impl AsDataManager,
    code: &str,
    source_code: &str,
) -> io::Result<()> {
    dm.delete_code_without_source(code, source_code).await
}

pub async fn delete_code_without_target(
    dm: &mut impl AsDataManager,
    code: &str,
    target_code: &str,
) -> io::Result<()> {
    dm.delete_code_without_target(code, target_code).await
}
