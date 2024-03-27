use crate::{
    data::AsDataManager,
    err::{Error, ErrorKind, Result},
};

use super::{graph::Path, inc, parser};

// Public
pub async fn dump_inc_v(dm: &mut impl AsDataManager, function: &str) -> Result<Vec<inc::Inc>> {
    let inc_h_v = dm.get_target_v(function, "inc").await?;
    let mut inc_v = Vec::with_capacity(inc_h_v.len());
    for inc_h in &inc_h_v {
        inc_v.push(inc::Inc {
            output: dm.get_target(inc_h, "output").await?,
            operator: dm.get_target(inc_h, "operator").await?,
            function: dm.get_target(inc_h, "function").await?,
            input: dm.get_target(inc_h, "input").await?,
            input1: dm.get_target(inc_h, "input1").await?,
        });
    }
    Ok(inc_v)
}

#[async_recursion::async_recursion]
pub async fn get_all_by_path(dm: &mut impl AsDataManager, mut path: Path) -> Result<Vec<String>> {
    if path.step_v.is_empty() {
        if path.root.is_empty() {
            return Ok(Vec::new());
        } else {
            return Ok(vec![path.root.clone()]);
        }
    }
    let root = path.root.clone();
    let step = path.step_v.remove(0);
    let curr_v = if step.arrow == "->" {
        dm.get_target_v(&root, &step.code).await?
    } else {
        dm.get_source_v(&step.code, &root).await?
    };
    let mut rs = Vec::new();
    for root in curr_v {
        rs.append(
            &mut get_all_by_path(
                dm,
                Path {
                    root,
                    step_v: path.step_v.clone(),
                },
            )
            .await?,
        );
    }
    Ok(rs)
}

pub async fn asign(
    dm: &mut impl AsDataManager,
    output: &str,
    operator: &str,
    item_v: Vec<String>,
) -> Result<()> {
    let mut output_path = Path::from_str(output);
    let last_step = output_path.step_v.pop().unwrap();
    let root_v = get_all_by_path(dm, output_path).await?;
    if last_step.arrow == "->" {
        for source in &root_v {
            if operator == "=" {
                dm.set_target_v(source, &last_step.code, &item_v).await?;
            } else {
                dm.append_target_v(source, &last_step.code, &item_v).await?;
            }
        }
    } else {
        for target in &root_v {
            if operator == "=" {
                dm.set_source_v(&item_v, &last_step.code, target).await?;
            } else {
                dm.append_source_v(&item_v, &last_step.code, target).await?;
            }
        }
    }
    Ok(())
}

pub async fn get_one(dm: &mut impl AsDataManager, root: &str, id: &str) -> Result<String> {
    let path = parser::unwrap_value(root, id);
    let id_v = get_all_by_path(dm, Path::from_str(&path)).await?;
    if id_v.len() != 1 {
        return Err(Error::new(ErrorKind::Other, "Not found".to_string()));
    }
    Ok(id_v[0].clone())
}
