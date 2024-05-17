use std::io::{self, Error, ErrorKind};

use sqlx::{MySql, Pool, Row};

// Public
pub async fn delete_edge_with_source_code(
    pool: Pool<MySql>,
    source: &str,
    code: &str,
) -> io::Result<()> {
    sqlx::query("delete from edge_t where source = ? and code = ?")
        .bind(source)
        .bind(code)
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}

pub async fn insert_edge(
    pool: Pool<MySql>,
    source: &str,
    code: &str,
    target_v: &Vec<String>,
) -> io::Result<()> {
    if target_v.is_empty() {
        return Ok(());
    }
    log::info!("commit target_v: {}", target_v.len());
    let value_v = target_v
        .iter()
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
    for target in target_v {
        statement = statement.bind(source).bind(code).bind(target);
    }

    statement
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn get_target_v(pool: Pool<MySql>, source: &str, code: &str) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select target from edge_t where source=? and code=? order by id")
        .bind(source)
        .bind(code)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

pub async fn get_source_v(pool: Pool<MySql>, code: &str, target: &str) -> io::Result<Vec<String>> {
    let rs = sqlx::query("select source from edge_t where code=? and target=? order by id")
        .bind(code)
        .bind(target)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}
