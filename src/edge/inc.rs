use std::io;

use crate::{data::AsDataManager, mem_table::new_point};

fn find_arrrow(path: &str) -> usize {
    let p = path.find("->");
    let q = path.find("<-");
    if p.is_none() && q.is_none() {
        path.len()
    } else {
        if p.is_some() && q.is_some() {
            let p = p.unwrap();
            let q = q.unwrap();
            std::cmp::min(p, q)
        } else if p.is_some() {
            p.unwrap()
        } else {
            q.unwrap()
        }
    }
}

#[derive(Clone)]
struct Step {
    arrow: String,
    code: String,
}

// Public
pub struct Path {
    root: String,
    step_v: Vec<Step>,
}

impl Path {
    pub fn from_str(path: &str) -> Self {
        let mut s = find_arrrow(path);

        let root = path[0..s].to_string();
        if s == path.len() {
            return Self {
                root,
                step_v: Vec::new(),
            };
        }
        let mut tail = &path[s..];
        let mut step_v = Vec::new();
        loop {
            s = find_arrrow(&tail[2..]) + 2;
            step_v.push(Step {
                arrow: tail[0..2].to_string(),
                code: tail[2..s].to_string(),
            });
            if s == tail.len() {
                break;
            }
            tail = &tail[s..];
        }
        Self { root, step_v }
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

#[async_recursion::async_recursion]
pub async fn get_all_by_path(dm: &mut impl AsDataManager, mut path: Path) -> io::Result<Vec<String>> {
    if path.step_v.is_empty() {
        return Ok(vec![path.root.clone()]);
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

pub async fn clear(dm: &mut impl AsDataManager, source: &str, code: &str) -> io::Result<()> {
    let source_v = get_all_by_path(dm, Path::from_str(source)).await?;
    let code_v = get_all_by_path(dm, Path::from_str(code)).await?;
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
