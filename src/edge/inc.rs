use std::io;

use crate::{
    data::AsDataManager,
    edge::graph::get_or_empty,
    mem_table::new_point,
};

// Public
#[async_recursion::async_recursion]
pub async fn clear(
    dm: &mut impl AsDataManager,
    source: &str,
    code: &str,
) -> io::Result<()> {
    dm.clear(source, code).await
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
    } else if value == "$" {
        Ok(root.to_string())
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
