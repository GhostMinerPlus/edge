use std::io::{self, Error, ErrorKind};

use edge_lib::{Path, Step};
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

pub async fn get(pool: Pool<MySql>, path: &Path) -> io::Result<Vec<String>> {
    let first_step = &path.step_v[0];
    let sql = gen_sql_stm(first_step, &path.step_v[1..]);
    let mut stm = sqlx::query(&sql).bind(&path.root);
    for step in &path.step_v {
        stm = stm.bind(&step.code);
    }
    let rs = stm
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

fn gen_sql_stm(first_step: &Step, step_v: &[Step]) -> String {
    let sql = if first_step.arrow == "->" {
        format!(
            "select {}_v.root from (select target as root from edge_t where source=? and code=?) 0_v",
            step_v.len(),
        )
    } else {
        format!(
            "select {}_v.root from (select source as root from edge_t where target=? and code=?) 0_v",
            step_v.len(),
        )
    };
    let mut root = format!("0_v");
    let mut no = 0;
    let join_v = step_v.iter().map(|step| {
        let p_root = root.clone();
        root = format!("{no}_v");
        no += 1;
        if step.arrow == "->" {
            format!(
                "join (select target as root, source from edge_t where code=?) {no}_v on {no}_v.source = {p_root}.root",
            )
        } else {
            format!(
                "join (select source as root, target from edge_t where code=?) {no}_v on {no}_v.source = {p_root}.root",
            )
        }
    }).reduce(|acc, item| {
        format!("{acc}\n{item}")
    }).unwrap_or_default();
    format!("{sql}\n{join_v}")
}

#[test]
fn test_gen_sql() {
    let sql = gen_sql_stm(
        &Step {
            arrow: "->".to_string(),
            code: "code".to_string(),
        },
        &vec![
            Step {
                arrow: "->".to_string(),
                code: "code".to_string(),
            }
        ],
    );
    println!("{sql}")
}
