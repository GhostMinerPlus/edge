mod dao;

use std::io;

use serde::Deserialize;
use sqlx::MySqlConnection;

pub use dao::new_point;

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
            dao::set(conn, &root, &inc.output, &inc.input).await?;
        }
        "delete" => {
            dao::delete_edge(conn, &inc.input).await?;
        }
        "insert" => {
            let source = dao::get_target(conn, &inc.input, "source").await?;
            let code = dao::get_target(conn, &inc.input, "code").await?;
            let target = dao::get_target(conn, &inc.input, "target").await?;
            let id = dao::insert_edge(conn, &source, &code, &target).await?;
            dao::set(conn, root, &inc.output, &id).await?;
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

pub async fn unwrap_inc(conn: &mut MySqlConnection, root: &str, inc: &Inc) -> io::Result<Inc> {
    Ok(Inc {
        code: dao::unwrap_value(conn, root, &inc.code).await?,
        input: dao::unwrap_value(conn, root, &inc.input).await?,
        output: dao::unwrap_value(conn, root, &inc.output).await?,
    })
}
