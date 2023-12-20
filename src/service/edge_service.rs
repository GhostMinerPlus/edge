use std::io::{self, Error, ErrorKind};

use edge::Edge;
use serde::Deserialize;
use sqlx::{MySqlConnection, Row};

#[derive(Deserialize)]
pub struct EdgeForm {
    context: String,
    source: String,
    code: String,
    target: String,
    no: u64,
}

impl Default for EdgeForm {
    fn default() -> Self {
        Self {
            context: Default::default(),
            source: Default::default(),
            code: Default::default(),
            target: Default::default(),
            no: 1,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct Inc {
    code: String,
    input: String,
    output: String,
}

async fn insert_edge(conn: &mut MySqlConnection, edge_form: &EdgeForm) -> io::Result<Edge> {
    // new edge
    let edge = Edge {
        id: new_point(),
        context: edge_form.context.clone(),
        source: edge_form.source.clone(),
        code: edge_form.code.clone(),
        target: edge_form.target.clone(),
        no: edge_form.no,
    };
    // insert
    sqlx::query("insert into edge_t (id,context,source,code,target,no) values (?,?,?,?,?,?)")
        .bind(&edge.id)
        .bind(&edge.context)
        .bind(&edge.source)
        .bind(&edge.code)
        .bind(&edge.target)
        .bind(edge_form.no)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(edge)
}

async fn get_target(
    conn: &mut MySqlConnection,
    context: &str,
    source: &str,
    code: &str,
) -> io::Result<String> {
    let row = sqlx::query(
        "select target from edge_t where context=? and source=? and code=? order by no",
    )
    .bind(context)
    .bind(source)
    .bind(code)
    .fetch_one(conn)
    .await
    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(row.get(0))
}

async fn get_source(
    conn: &mut MySqlConnection,
    context: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    let row = sqlx::query("select source from edge_t where context=? and code=? and target=?")
        .bind(context)
        .bind(code)
        .bind(target)
        .fetch_one(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(row.get(0))
}

async fn delete_edge(conn: &mut MySqlConnection, target: &str) -> io::Result<()> {
    log::info!("deleting edge:{target}");

    sqlx::query("delete from edge_t where id=?")
        .bind(target)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

async fn set(
    conn: &mut MySqlConnection,
    ctx: &str,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<()> {
    insert_edge(
        conn,
        &EdgeForm {
            context: ctx.to_string(),
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn get(
    conn: &mut MySqlConnection,
    ctx: &str,
    source: &str,
    code: &str,
) -> io::Result<String> {
    get_target(conn, ctx, source, code).await
}

#[async_recursion::async_recursion]
async fn invoke_inc(
    conn: &mut MySqlConnection,
    ctx: &str,
    stack: &mut Vec<String>,
    root: &mut String,
    inc: &Inc,
) -> io::Result<()> {
    match inc.code.as_str() {
        "set" => {
            set(conn, ctx, &root, &inc.output, &inc.input).await?;
        }
        "push" => {
            stack.push(inc.input.clone());
            let pt = new_point();
            set(conn, ctx, &root, &inc.input, &pt).await?;
            *root = pt;
        }
        "pop" => {
            let input = root.clone();
            let name = stack.pop().unwrap();
            *root = get_source(conn, ctx, &name, &root).await?;
            invoke_inc(
                conn,
                ctx,
                stack,
                root,
                &Inc {
                    code: inc.input.clone(),
                    input,
                    output: inc.output.clone(),
                },
            )
            .await?;
        }
        "delete" => {
            delete_edge(conn, &inc.input).await?;
        }
        "insert" => {
            let source = get_target(conn, ctx, &inc.input, "source").await?;
            let code = get_target(conn, ctx, &inc.input, "code").await?;
            let target = get_target(conn, ctx, &inc.input, "target").await?;
            let edge = insert_edge(
                conn,
                &EdgeForm {
                    context: ctx.to_string(),
                    source,
                    code,
                    target,
                    ..Default::default()
                },
            )
            .await?;
            set(conn, ctx, root, &inc.output, &edge.id).await?;
        }
        _ => {
            todo!("");
        }
    }
    Ok(())
}

async fn unwrap(conn: &mut MySqlConnection, ctx: &str, root: &str, inc: &Inc) -> io::Result<Inc> {
    let mut unwraped_inc = inc.clone();
    if inc.code.starts_with("@") {
        unwraped_inc.code = get(conn, ctx, root, &inc.code[1..]).await?;
    }
    if inc.input.starts_with("@") {
        unwraped_inc.input = get(conn, ctx, root, &inc.input[1..]).await?;
    }
    return Ok(unwraped_inc);
}

async fn invoke_inc_v(
    conn: &mut MySqlConnection,
    ctx: &str,
    stack: &mut Vec<String>,
    root: &mut String,
    inc_v: &Vec<Inc>,
) -> io::Result<String> {
    for inc in inc_v {
        let inc = unwrap(conn, ctx, &root, inc).await?;
        if inc.code.as_str() == "return" {
            return Ok(inc.input);
        } else {
            invoke_inc(conn, ctx, stack, root, &inc).await?;
        }
    }
    Ok(String::new())
}

pub async fn execute(conn: &mut MySqlConnection, inc_v: &Vec<Inc>) -> io::Result<String> {
    let ctx = new_point();
    let mut stack = Vec::new();
    let mut root = "root".to_string();
    invoke_inc_v(conn, &ctx, &mut stack, &mut root, inc_v).await
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sqlx::{Acquire, MySql, Pool};

    use crate::Config;

    fn init() {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("INFO"))
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
            super::execute(
                &mut conn,
                &serde_json::from_str(
                    r#"[
    {
        "code": "push",
        "data": "edge",
        "output": ""
    },
    {
        "code": "set",
        "data": "xxx",
        "output": "source"
    },
    {
        "code": "set",
        "data": "xxx",
        "output": "code"
    },
    {
        "code": "set",
        "data": "xxx",
        "output": "target"
    },
    {
        "code": "pop",
        "data": "insert",
        "output": "edge"
    },
    {
        "code": "delete",
        "data": "edge",
        "output": ""
    }
]"#,
                )
                .unwrap(),
            )
            .await
            .unwrap();
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
