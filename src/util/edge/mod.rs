mod inc;

use serde::Deserialize;
use sqlx::MySqlConnection;
use std::io;

async fn invoke_script(
    conn: &mut MySqlConnection,
    root: &mut String,
    mut inc_v_h: String,
) -> io::Result<String> {
    let mut inc_v = Vec::new();
    loop {
        inc_v_h = match inc::get_object(conn, &inc_v_h, "next").await {
            Ok(r) => r,
            Err(_) => break,
        };
        let subject = inc::get_object(conn, &inc_v_h, "subject").await?;
        let predicate = inc::get_object(conn, &inc_v_h, "predicate").await?;
        let object = inc::get_object(conn, &inc_v_h, "object").await?;
        inc_v.push(Inc {
            subject,
            predicate,
            object,
        });
    }
    if inc_v.is_empty() {
        Ok(inc_v_h)
    } else {
        invoke_inc_v(conn, root, &inc_v).await
    }
}

#[derive(Clone, Deserialize)]
pub struct Inc {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

#[async_recursion::async_recursion]
pub async fn invoke_inc(
    conn: &mut MySqlConnection,
    root: &mut String,
    inc: &Inc,
) -> io::Result<i32> {
    match inc.predicate.as_str() {
        "set" => {
            inc::set(conn, &root, &inc.subject, &inc.object).await?;
        }
        "delete" => {
            inc::delete_edge(conn, &inc.object).await?;
        }
        "insert" => {
            let subject = inc::get_object(conn, &inc.object, "subject").await?;
            let predicate = inc::get_object(conn, &inc.object, "predicate").await?;
            let object = inc::get_object(conn, &inc.object, "object").await?;
            let id = inc::insert_edge(conn, &subject, &predicate, &object).await?;
            inc::set(conn, root, &inc.subject, &id).await?;
        }
        "cmp" => {
            let left: f64 = inc::get_object(conn, &inc.object, "left")
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc::get_object(conn, &inc.object, "right")
                .await?
                .parse()
                .unwrap();

            let r = if left < right {
                "1"
            } else if left > right {
                "3"
            } else {
                "2"
            };
            inc::set(conn, root, &inc.subject, r).await?;
        }
        "add" => {
            let left: f64 = inc::get_object(conn, &inc.object, "left")
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc::get_object(conn, &inc.object, "right")
                .await?
                .parse()
                .unwrap();

            let r = left + right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "minus" => {
            let left: f64 = inc::get_object(conn, &inc.object, "left")
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc::get_object(conn, &inc.object, "right")
                .await?
                .parse()
                .unwrap();

            let r = left - right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "mul" => {
            let left: f64 = inc::get_object(conn, &inc.object, "left")
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc::get_object(conn, &inc.object, "right")
                .await?
                .parse()
                .unwrap();

            let r = left * right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "div" => {
            let left: f64 = inc::get_object(conn, &inc.object, "left")
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc::get_object(conn, &inc.object, "right")
                .await?
                .parse()
                .unwrap();

            let r = left / right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "mod" => {
            let left: u64 = inc::get_object(conn, &inc.object, "left")
                .await?
                .parse()
                .unwrap();
            let right: u64 = inc::get_object(conn, &inc.object, "right")
                .await?
                .parse()
                .unwrap();

            let r = left % right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "jump" => {
            let step: i32 = inc.object.parse().unwrap();
            return Ok(step);
        }
        _ => {
            let r = invoke_script(conn, &mut inc.object.clone(), inc.predicate.clone()).await?;
            inc::set(conn, root, &inc.subject, &r).await?;
        }
    }
    Ok(1)
}

pub async fn unwrap_inc(conn: &mut MySqlConnection, root: &str, inc: &Inc) -> io::Result<Inc> {
    Ok(Inc {
        subject: inc::unwrap_value(conn, root, &inc.subject).await?,
        predicate: inc::unwrap_value(conn, root, &inc.predicate).await?,
        object: inc::unwrap_value(conn, root, &inc.object).await?,
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
        if inc.predicate.as_str() == "return" {
            return Ok(inc.object);
        } else {
            pos += invoke_inc(conn, root, &inc).await?;
        }
    }
    Ok(String::new())
}
