use std::io;

use crate::{data::AsDataManager, mem_table::new_point};

#[async_recursion::async_recursion]
async fn get(dm: &mut impl AsDataManager, root: &str, path: &str) -> io::Result<String> {
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
                dm.get_target(root, code).await?
            } else {
                dm.get_source(code, root).await?
            };
            get(dm, &pt, path).await
        } else {
            if arrow == "->" {
                dm.get_target(root, path).await
            } else {
                dm.get_source(path, root).await
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

        get(dm, root, path).await
    }
}

// Public
pub async fn get_or_empty(
    dm: &mut impl AsDataManager,
    root: &str,
    path: &str,
) -> io::Result<String> {
    match get(dm, root, path).await {
        Ok(r) => Ok(r),
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => Ok(String::new()),
            _ => Err(e),
        },
    }
}

pub async fn get_target_anyway(
    dm: &mut impl AsDataManager,
    source: &str,
    code: &str,
) -> io::Result<String> {
    match dm.get_target(source, code).await {
        Ok(target) => Ok(target),
        Err(_) => {
            let target = new_point();
            dm.insert_edge(source, code, &target).await?;
            Ok(target)
        }
    }
}

pub async fn get_source_anyway(
    dm: &mut impl AsDataManager,
    code: &str,
    target: &str,
) -> io::Result<String> {
    match dm.get_source(code, target).await {
        Ok(source) => Ok(source),
        Err(_) => {
            let source = new_point();
            dm.insert_edge(&source, code, target).await?;
            Ok(source)
        }
    }
}
