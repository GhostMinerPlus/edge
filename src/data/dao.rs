use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use sqlx::{MySqlConnection, Row};

use crate::{edge::Path, mem_table::Edge};

fn path_2_sql(path: &Path) -> Option<String> {
    if path.step_v.is_empty() {
        return None;
    }
    let step0 = &path.step_v[0];
    let (mut root, sql) = if step0.arrow == "->" {
        if path.step_v.len() == 1 {
            return Some(format!(
                "select id, target from edge_t where code = '{}' and source = '{}'",
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
                "select id, source as target from edge_t where code = '{}' and target = '{}'",
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
    let mut name = "v".to_string();
    let join_v = path.step_v[1..]
        .into_iter()
        .enumerate()
        .map(|(i, step)| {
            name = format!("v{i}");
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
    Some(format!(
        "select {name}.id, {root} as target from {sql} {join_v}"
    ))
}

fn make_dump_sql(path: &str, item_v: &Vec<String>) -> String {
    let attr_item_v = item_v
        .into_iter()
        .enumerate()
        .map(|(i, attr)| format!("v{i}.target as {attr}"))
        .reduce(|acc, item| format!("{acc},{item}"))
        .unwrap();
    if let Some(path) = path_2_sql(&Path::from_str(path)) {
        let attr_join_v = item_v
        .into_iter()
        .enumerate()
        .map(|(i, item)| format!("join (select * from edge_t where code = '{item}') v{i} on v{i}.source = path.target"))
        .reduce(|acc, item| format!("{acc}\n{item}"))
        .unwrap();
        format!("select {attr_item_v} from ({path}) path {attr_join_v} ORDER BY path.id")
    } else {
        let attr_join_v = item_v
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                format!(
                    "(select * from edge_t where code = '{item}') v{i} on v{i}.source = '{path}'"
                )
            })
            .fold(String::default(), |acc, item| {
                if acc.is_empty() {
                    item
                } else {
                    format!("{acc}\njoin {item}")
                }
            });
        format!(
            "SELECT {attr_item_v}
FROM {attr_join_v}
ORDER BY path.id"
        )
    }
}

// Public
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

pub async fn delete_edge_with_code_target(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<()> {
    sqlx::query("delete from edge_t where code = ? and target = ?")
        .bind(code)
        .bind(target)
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
        .map(|_| format!("(?,?,?)"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let sql = format!("insert into edge_t (source,code,target) values {value_v}");
    let mut statement = sqlx::query(&sql);
    for (_, edge) in edge_mp {
        statement = statement
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
) -> io::Result<String> {
    let row =
        sqlx::query("select target from edge_t where source=? and code=?  order by id limit 1")
            .bind(source)
            .bind(code)
            .fetch_one(conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
                _ => Error::new(ErrorKind::Other, e),
            })?;
    Ok(row.get(0))
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where source=? and code=? order by id")
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

pub async fn get_source_v(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select source from edge_t where code=? and target=? order by id")
        .bind(code)
        .bind(target)
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
) -> io::Result<String> {
    let row =
        sqlx::query("select source from edge_t where code=? and target=?  order by id limit 1")
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

pub async fn dump(
    conn: &mut MySqlConnection,
    path: &str,
    item_v: &Vec<String>,
) -> io::Result<json::Array> {
    let mut arr = json::Array::new();
    let sql = make_dump_sql(path, item_v);
    log::debug!("{sql}");
    let rs = sqlx::query(&sql)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    for row in rs {
        let mut obj = json::object! {};
        for i in 0..item_v.len() {
            let attr = &item_v[i];
            obj[attr] = json::JsonValue::String(row.get(i));
        }
        arr.push(obj);
    }
    Ok(arr)
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
    use super::make_dump_sql;

    #[test]
    fn test() {
        let sql = make_dump_sql("huiwen->canvas->point", &vec!["color".to_string()]);
        println!("{sql}");
    }
}
