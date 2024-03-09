use std::io;

use crate::{data::AsDataManager, mem_table::new_point};

// Public
pub async fn clear(dm: &mut impl AsDataManager, source: &str, code: &str) -> io::Result<()> {
    let source_v = dm.get_all_by_path(source).await?;
    let code_v = dm.get_all_by_path(code).await?;
    for source in &source_v {
        for code in &code_v {
            dm.clear(source, code).await?;
        }
    }
    Ok(())
}

pub async fn dump(dm: &mut impl AsDataManager, target: &str) -> io::Result<String> {
    let root = dm.get_target(target, "$root").await?;
    let dimension_v = dm.get_target_v(target, "$dimension").await?;
    let attr_v = dm.get_target_v(target, "$attr").await?;

    let rs = dm.get_list(&root, &dimension_v, &attr_v).await?;
    Ok(json::stringify(rs))
}

pub async fn unwrap_value(root: &str, value: &str) -> io::Result<String> {
    if value == "?" {
        Ok(new_point())
    } else if value == "$" {
        Ok(root.to_string())
    } else {
        Ok(value
            .replace("$->", &format!("{root}->"))
            .replace("$<-", &format!("{root}<-")))
    }
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
