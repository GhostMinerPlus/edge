use std::io::{self, Error, ErrorKind};

use serde::Deserialize;
use sqlx::{MySqlConnection, Row};

#[derive(Clone, Deserialize)]
pub struct Inc {
    code: String,
    input: String,
    output: String,
}

async fn insert_edge(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    log::debug!("insert_edge: {source}->{code}={target}");

    let id = new_point();
    sqlx::query("insert into edge_t (id,source,code,target) values (?,?,?,?)")
        .bind(&id)
        .bind(&source)
        .bind(&code)
        .bind(&target)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(id)
}

async fn delete_code(conn: &mut MySqlConnection, source: &str, code: &str) -> io::Result<()> {
    log::debug!("delete_code: {source}->{code}");

    sqlx::query("delete from edge_t where source=? and code=?")
        .bind(&source)
        .bind(&code)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

async fn set_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    delete_code(conn, source, code).await?;
    insert_edge(conn, source, code, target).await
}

async fn get_target_anyway(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<String> {
    match get_target(conn, source, code).await {
        Ok(target) => Ok(target),
        Err(_) => {
            let target = new_point();
            insert_edge(conn, source, code, &target).await?;
            Ok(target)
        }
    }
}

async fn get_target(conn: &mut MySqlConnection, source: &str, code: &str) -> io::Result<String> {
    log::debug!("get_target: {source}->{code}=?");

    let row = sqlx::query("select target from edge_t where source=? and code=?")
        .bind(source)
        .bind(code)
        .fetch_one(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let target = row.get(0);
    log::debug!("get_target: {source}->{code}={target}");

    Ok(target)
}

async fn get_source_anyway(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<String> {
    match get_source(conn, code, target).await {
        Ok(source) => Ok(source),
        Err(_) => {
            let source = new_point();
            insert_edge(conn, &source, code, target).await?;
            Ok(source)
        }
    }
}

async fn get_source(conn: &mut MySqlConnection, code: &str, target: &str) -> io::Result<String> {
    let row = sqlx::query("select source from edge_t where code=? and target=?")
        .bind(code)
        .bind(target)
        .fetch_one(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(row.get(0))
}

async fn delete_edge(conn: &mut MySqlConnection, id: &str) -> io::Result<()> {
    log::info!("deleting edge:{id}");

    sqlx::query("delete from edge_t where id=?")
        .bind(id)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[async_recursion::async_recursion]
async fn set(
    conn: &mut MySqlConnection,
    root: &str,
    path: &str,
    value: &str,
) -> io::Result<String> {
    if path.is_empty() {
        return Ok(String::new());
    }

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
            get_target_anyway(conn, root, code).await?
        } else {
            get_source_anyway(conn, code, root).await?
        };
        set(conn, &pt, path, value).await
    } else {
        set_target(conn, root, path, value).await
    }
}

#[async_recursion::async_recursion]
async fn get(conn: &mut MySqlConnection, root: &str, path: &str) -> io::Result<String> {
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
        let path = &path[pos + 2..];

        let pt = if arrow == "->" {
            get_target_anyway(conn, root, code).await?
        } else {
            get_source_anyway(conn, code, root).await?
        };
        get(conn, &pt, path).await
    } else {
        if arrow == "->" {
            get_target_anyway(conn, root, path).await
        } else {
            get_source_anyway(conn, path, root).await
        }
    }
}

#[async_recursion::async_recursion]
async fn invoke_inc(conn: &mut MySqlConnection, root: &mut String, inc: &Inc) -> io::Result<()> {
    match inc.code.as_str() {
        "set" => {
            set(conn, &root, &inc.output, &inc.input).await?;
        }
        "delete" => {
            delete_edge(conn, &inc.input).await?;
        }
        "insert" => {
            let source = get_target(conn, &inc.input, "source").await?;
            let code = get_target(conn, &inc.input, "code").await?;
            let target = get_target(conn, &inc.input, "target").await?;
            let id = insert_edge(conn, &source, &code, &target).await?;
            set(conn, root, &inc.output, &id).await?;
        }
        _ => {
            // let f_h = get_target(conn, "root", "fn").await?;
            // let inc_h_v = get_target_v(conn, &f_h, &inc.code).await?;
            // let mut inc_v = Vec::new();
            // for inc_h in &inc_h_v {
            //     let code = get_target(conn, inc_h, "code").await?;
            //     let input = get_target(conn, inc_h, "input").await?;
            //     let output = get_target(conn, inc_h, "output").await?;
            //     inc_v.push(Inc {
            //         code,
            //         input,
            //         output,
            //     });
            // }
            // invoke_inc_v(conn, root, &inc_v).await?;
        }
    }
    Ok(())
}

async fn unwrap_value(conn: &mut MySqlConnection, root: &str, value: &str) -> io::Result<String> {
    if value.starts_with("->") || value.starts_with("<-") {
        get(conn, root, value).await
    } else if value.starts_with("\"") {
        Ok(value[1..value.len() - 1].to_string())
    } else {
        Ok(value.to_string())
    }
}

async fn unwrap_inc(conn: &mut MySqlConnection, root: &str, inc: &Inc) -> io::Result<Inc> {
    Ok(Inc {
        code: unwrap_value(conn, root, &inc.code).await?,
        input: unwrap_value(conn, root, &inc.input).await?,
        output: unwrap_value(conn, root, &inc.output).await?,
    })
}

async fn invoke_inc_v(
    conn: &mut MySqlConnection,
    root: &mut String,
    inc_v: &Vec<Inc>,
) -> io::Result<String> {
    for inc in inc_v {
        let inc = unwrap_inc(conn, &root, inc).await?;
        if inc.code.as_str() == "return" {
            return Ok(inc.input);
        } else {
            invoke_inc(conn, root, &inc).await?;
        }
    }
    Ok(String::new())
}

pub async fn execute(conn: &mut MySqlConnection, script: &str) -> io::Result<String> {
    let mut root = "root".to_string();
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        let mut word_v: Vec<&str> = line.split(" ").collect();
        while word_v.len() < 3 {
            word_v.push("");
        }
        inc_v.push(Inc {
            code: word_v[0].to_string(),
            input: word_v[1].to_string(),
            output: word_v[2].to_string(),
        });
    }
    invoke_inc_v(conn, &mut root, &inc_v).await
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sqlx::{Acquire, MySql, Pool};

    use crate::Config;

    fn init() {
        let _ =
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("DEBUG"))
                .is_test(true)
                .try_init();
    }

    #[test]
    fn test_execute() {
        init();
        let f = async {
            let config: Config =
                toml::from_str(&fs::read_to_string("config.toml").unwrap()).unwrap();
            let pool: Pool<MySql> = sqlx::Pool::connect(&config.db_url).await.unwrap();

            let mut tr = pool.begin().await.unwrap();
            let mut conn = tr.acquire().await.unwrap();
            let r = super::execute(
                &mut conn,
                r#"set xxx "->edge->source"
set xxx "->edge->code"
set xxx "->edge->target"
insert ->edge "->id"
delete ->id
return ->id"#,
            )
            .await;
            tr.rollback().await.unwrap();
            assert!(!r.unwrap().is_empty());
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn test_new_point() {
        init();
        let f = async {
            let id = super::new_point();
            let id1 = super::new_point();
            assert_ne!(id, id1);
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }
}
