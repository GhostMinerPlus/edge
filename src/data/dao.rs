use std::{
    cmp::min,
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use sqlx::{MySqlConnection, Row};
use toml::to_string;

use crate::mem_table::Edge;

struct Step {
    arrow: String,
    code: String,
}

fn find_arrrow(path: &str) -> usize {
    let p = path.find("->");
    let q = path.find("<-");
    if p.is_none() && q.is_none() {
        path.len()
    } else {
        if p.is_some() && q.is_some() {
            let p = p.unwrap();
            let q = q.unwrap();
            min(p, q)
        } else if p.is_some() {
            p.unwrap()
        } else {
            q.unwrap()
        }
    }
}

fn path_2_sql(path: &Path) -> Option<String> {
    if path.step_v.is_empty() {
        return None;
    }
    let step0 = &path.step_v[0];
    let (mut root, sql) = if step0.arrow == "->" {
        if path.step_v.len() == 1 {
            return Some(format!(
                "select target from edge_t where code = '{}' and source = '{}'",
                step0.code, path.root
            ));
        }
        (
            format!("v.target"),
            format!(
                "(select * from edge_t where code = '{}' and source = '{}') v",
                step0.code, path.root
            ),
        )
    } else {
        if path.step_v.len() == 1 {
            return Some(format!(
                "select source from edge_t where code = '{}' and target = '{}'",
                step0.code, path.root
            ));
        }
        (
            format!("v.source"),
            format!(
                "(select * from edge_t where code = '{}' and target = '{}') v",
                step0.code, path.root
            ),
        )
    };
    let join_v = path.step_v[1..]
        .into_iter()
        .enumerate()
        .map(|(i, step)| {
            let name = format!("v{i}");
            let (cur, sql) = if step.arrow == "->" {
                (
                    format!("{name}.target"),
                    format!(
                        "join (select * from edge_t where code = '{}') {name} on {name}.source = {root}",
                        step.code,
                    ),
                )
            } else {
                (
                    format!("{name}.source"),
                    format!(
                        "join (select * from edge_t where code = '{}') {name} on {name}.target = {root}",
                        step.code,
                    ),
                )
            };
            root = cur;
            sql
        })
        .reduce(|acc, item| format!("{acc}\n{item}")).unwrap();
    Some(format!("select {root} from {sql} {join_v}"))
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

pub async fn get_all_by_path(conn: &mut MySqlConnection, s: &str) -> io::Result<Vec<String>> {
    let path = Path::from_str(s);
    match path_2_sql(&path) {
        Some(sql) => {
            let row_v = sqlx::query(&sql)
                .fetch_all(conn)
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e))?;
            let mut rs = Vec::with_capacity(row_v.len());
            for row in row_v {
                rs.push(row.get(0));
            }
            Ok(rs)
        }
        None => Ok(vec![s.to_string()]),
    }
}

pub async fn get_one_by_path(conn: &mut MySqlConnection, s: &str) -> io::Result<String> {
    let path = Path::from_str(s);
    match path_2_sql(&path) {
        Some(sql) => {
            let row = sqlx::query(&sql)
                .fetch_one(conn)
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e))?;
            Ok(row.get(0))
        }
        None => Ok(s.to_string()),
    }
}

pub async fn delete_edge_with_source_code(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<()> {
    sqlx::query("delete from edge_t where source = ? and code = ?")
        .bind(source)
        .bind(code)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn insert_edge_mp(
    conn: &mut MySqlConnection,
    edge_mp: &HashMap<String, Edge>,
) -> io::Result<()> {
    if edge_mp.is_empty() {
        return Ok(());
    }
    log::info!("commit edge_mp: {}", edge_mp.len());
    let value_v = edge_mp
        .into_iter()
        .map(|_| format!("(?,?,?,?,?)"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let sql = format!("insert into edge_t (id,source,code,target) values {value_v}");
    let mut statement = sqlx::query(&sql);
    for (_, edge) in edge_mp {
        statement = statement
            .bind(&edge.id)
            .bind(&edge.source)
            .bind(&edge.code)
            .bind(&edge.target);
    }

    statement
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn get_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<(String, String)> {
    let row = sqlx::query(
        "select id, target from edge_t where source=? and code=?  order by creation_time limit 1",
    )
    .bind(source)
    .bind(code)
    .fetch_one(conn)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
        _ => Error::new(ErrorKind::Other, e),
    })?;
    Ok((row.get(0), row.get(1)))
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let rs =
        sqlx::query("select target from edge_t where source=? and code=? order by creation_time")
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

pub async fn get_source(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<(String, String)> {
    let row = sqlx::query(
        "select id, source from edge_t where code=? and target=?  order by creation_time limit 1",
    )
    .bind(code)
    .bind(target)
    .fetch_one(conn)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
        _ => Error::new(ErrorKind::Other, e),
    })?;
    Ok((row.get(0), row.get(1)))
}

pub async fn get_list(
    conn: &mut MySqlConnection,
    root: &str,
    dimension_v: &Vec<String>,
    attr_v: &Vec<String>,
) -> io::Result<json::Array> {
    let mut arr = json::Array::new();
    let dimension_item_v = dimension_v
        .into_iter()
        .map(|dimension| format!("{dimension}_t.target as {dimension}"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let attr_item_v = attr_v
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
    let mut pre_dimension = String::new();
    let dimension_join_v = dimension_v
        .into_iter()
        .enumerate()
        .map(|(i, dimension)| {
            if i == 0 {
                pre_dimension = dimension.clone();
                format!("(select * from edge_t where code = '{dimension}' and source = '{root}') {dimension}_t")
            } else {
                let r = format!("(select * from edge_t where code = '{dimension}') {dimension}_t on {dimension}_t.source = {pre_dimension}_t.target");
                pre_dimension = dimension.clone();
                return r;
            }
        })
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc}\njoin {item}")
            }
        })
        .unwrap();
    let last_dimension = dimension_v.last().unwrap().clone();
    let attr_join_v = attr_v
        .into_iter()
        .map(|attr| format!("(select * from edge_t where code = '{attr}') {attr}_t on {attr}_t.source = {last_dimension}_t.target"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc}\njoin {item}")
            }
        })
        .unwrap();
    let sql = format!(
        "SELECT {dimension_item_v}, {attr_item_v}
FROM {dimension_join_v}
join {attr_join_v}
order by {last_dimension}_t.creation_time"
    );
    log::debug!("{sql}");
    let rs = sqlx::query(&sql)
        .bind(root)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    for row in rs {
        let mut obj = json::object! {};
        for i in 0..dimension_v.len() {
            let attr = &dimension_v[i];
            obj[attr] = json::JsonValue::String(row.get(i));
        }
        for i in dimension_v.len()..dimension_v.len() + attr_v.len() {
            let attr = &attr_v[i - dimension_v.len()];
            obj[attr] = json::JsonValue::String(row.get(i));
        }
        arr.push(obj);
    }
    Ok(arr)
}

pub async fn delete(conn: &mut MySqlConnection, point: &str) -> io::Result<()> {
    sqlx::query("DELETE FROM edge_t WHERE source = ? OR code = ? OR target = ?")
        .bind(point)
        .bind(point)
        .bind(point)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_code(conn: &mut MySqlConnection, code: &str) -> io::Result<()> {
    sqlx::query("delete from edge_t where code = ?")
        .bind(code)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_code_without_source(
    conn: &mut MySqlConnection,
    code: &str,
    source_code: &str,
) -> io::Result<()> {
    sqlx::query(
        "delete from edge_t
where code = ?
and not exists (
select 1 from edge_t v where v.code = ? and v.target = source
)",
    )
    .bind(code)
    .bind(source_code)
    .execute(conn)
    .await
    .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn delete_code_without_target(
    conn: &mut MySqlConnection,
    code: &str,
    target_code: &str,
) -> io::Result<()> {
    sqlx::query(
        "delete from edge_t
where code = ?
and not exists (
select 1 from edge_t v where v.code = ? and v.source = target
)",
    )
    .bind(code)
    .bind(target_code)
    .execute(conn)
    .await
    .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{path_2_sql, Path};

    #[test]
    fn test() {
        let path = Path::from_str("52ab5814-e9af-485b-a389-5a3e4829ec51<-canvas->version");
        let sql = path_2_sql(&path).unwrap();
        println!("{sql}");
    }
}
