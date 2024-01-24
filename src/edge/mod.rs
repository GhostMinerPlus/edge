mod inc;

use serde::Deserialize;
use sqlx::MySqlConnection;
use std::io;

mod graph;

// Public
#[derive(Clone, Deserialize)]
pub struct Inc {
    pub source: String,
    pub code: String,
    pub target: String,
}

pub enum InvokeResult {
    Jump(i32),
    Return(String),
}

#[async_recursion::async_recursion]
pub async fn invoke_inc(
    conn: &mut MySqlConnection,
    root: &mut String,
    inc: &Inc,
) -> io::Result<InvokeResult> {
    match inc.code.as_str() {
        "return" => Ok(InvokeResult::Return(inc.target.clone())),
        "dump" => Ok(InvokeResult::Return(inc::dump(conn, &inc.target).await?)),
        "set" => {
            inc::set(conn, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "append" => {
            inc::append(conn, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        _ => todo!(),
    }
}

pub async fn unwrap_inc(conn: &mut MySqlConnection, root: &str, inc: &Inc) -> io::Result<Inc> {
    Ok(Inc {
        source: inc::unwrap_value(conn, root, &inc.source).await?,
        code: inc::unwrap_value(conn, root, &inc.code).await?,
        target: inc::unwrap_value(conn, root, &inc.target).await?,
    })
}

pub async fn invoke_inc_v(
    conn: &mut MySqlConnection,
    root: &mut String,
    inc_v: &Vec<Inc>,
) -> io::Result<String> {
    let mut pos = 0i32;
    while (pos as usize) < inc_v.len() {
        let inc = unwrap_inc(conn, &root, &inc_v[pos as usize]).await?;
        match invoke_inc(conn, root, &inc).await? {
            InvokeResult::Jump(step) => pos += step,
            InvokeResult::Return(s) => {
                return Ok(s);
            }
        }
    }
    Ok(String::new())
}
