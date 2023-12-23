mod inc;

use serde::Deserialize;
use sqlx::MySqlConnection;
use std::io;

#[derive(Clone, Deserialize)]
pub struct Inc {
    pub code: String,
    pub input: String,
    pub output: String,
}

#[async_recursion::async_recursion]
pub async fn invoke_inc(
    conn: &mut MySqlConnection,
    root: &mut String,
    inc: &Inc,
) -> io::Result<()> {
    match inc.code.as_str() {
        "set" => {
            inc::set(conn, &root, &inc.output, &inc.input).await?;
        }
        "delete" => {
            inc::delete_edge(conn, &inc.input).await?;
        }
        "insert" => {
            let source = inc::get_target(conn, &inc.input, "source").await?;
            let code = inc::get_target(conn, &inc.input, "code").await?;
            let target = inc::get_target(conn, &inc.input, "target").await?;
            let id = inc::insert_edge(conn, &source, &code, &target).await?;
            inc::set(conn, root, &inc.output, &id).await?;
        }
        _ => {
            let mut inc_v_h = inc.code.clone();
            let mut inc_v = Vec::new();
            loop {
                inc_v_h = match inc::get_target(conn, &inc_v_h, "next").await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let code = inc::get_target(conn, &inc_v_h, "code").await?;
                let input = inc::get_target(conn, &inc_v_h, "input").await?;
                let output = inc::get_target(conn, &inc_v_h, "output").await?;
                inc_v.push(Inc {
                    code,
                    input,
                    output,
                });
            }
            let r = invoke_inc_v(conn, &mut inc.input.clone(), &inc_v).await?;
            inc::set(conn, root, &inc.output, &r).await?;
        }
    }
    Ok(())
}

pub async fn unwrap_inc(conn: &mut MySqlConnection, root: &str, inc: &Inc) -> io::Result<Inc> {
    Ok(Inc {
        code: inc::unwrap_value(conn, root, &inc.code).await?,
        input: inc::unwrap_value(conn, root, &inc.input).await?,
        output: inc::unwrap_value(conn, root, &inc.output).await?,
    })
}

pub async fn invoke_inc_v(
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
