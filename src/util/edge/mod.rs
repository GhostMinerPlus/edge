mod inc;

use serde::Deserialize;
use sqlx::MySqlConnection;
use std::io;

use crate::util::graph;

use super::graph::get_list;

// Public
#[derive(Clone, Deserialize)]
pub struct Inc {
    pub source: String,
    pub code: String,
    pub target: String,
}

#[derive(Clone, Deserialize)]
pub struct Edge {
    pub source: String,
    pub code: String,
    pub no: u64,
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
    let (handler, class) = if let Ok(class) = graph::get_target(conn, &inc.code, "class").await
    {
        (inc.code.as_str(), class)
    } else {
        ("", inc.code.clone())
    };
    match class.as_str() {
        "return" => {
            if handler.is_empty() {
                return Ok(InvokeResult::Return(inc.target.clone()));
            }
            if let Ok(_) = graph::get_target(conn, handler, "json").await {
                // "" return --json huiwen->canvas->edge_v --dimension 2 --attr pos --attr color --attr width
                let mut iter = graph::get_target(conn, &inc.target, "class").await?;
                let dimension = graph::get_target(conn, &inc.target, "dimension")
                    .await?
                    .parse::<i32>()
                    .unwrap();
                let attr_v = graph::get_target_v(conn, &inc.target, "attr").await?;
                if dimension == 1 {
                    let arr = get_list(conn, &mut iter, &attr_v).await?;
                    return Ok(InvokeResult::Return(json::stringify(json::object! {
                        "last": iter,
                        "json": arr
                    })));
                } else if dimension == 2 {
                    let mut last = iter.clone();
                    let mut arr = json::Array::new();
                    while !iter.is_empty() {
                        let mut sub_iter = graph::get_target_or_empty(conn, &iter, "first").await?;
                        arr.push(json::JsonValue::Array(
                            get_list(conn, &mut sub_iter, &attr_v).await?,
                        ));
                        last = iter.clone();
                        iter = graph::get_target_or_empty(conn, &iter, "next").await?;
                    }
                    return Ok(InvokeResult::Return(json::stringify(json::object! {
                        "last": last,
                        "json": arr
                    })));
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported dimension",
                    ));
                }
            }
        }
        "set" => {
            inc::set(conn, &root, &inc.source, &inc.target).await?;
        }
        "append" => {
            inc::append(conn, &root, &inc.source, &inc.target).await?;
        }
        "delete" => {
            inc::delete_edge(conn, &inc.target).await?;
        }
        "cmp" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.source)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.target.parse().unwrap();
            let r = if left < right {
                "1"
            } else if left > right {
                "3"
            } else {
                "2"
            };
            inc::set(conn, root, &inc.source, r).await?;
        }
        "cmp_str" => {
            let left = graph::get_or_empty(conn, root, &inc.source).await?;
            let right = &inc.target;
            let r = if &left == right { "1" } else { "2" };
            inc::set(conn, root, &inc.source, r).await?;
        }
        "add" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.source)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.target.parse().unwrap();
            let r = left + right;
            inc::set(conn, root, &inc.source, &r.to_string()).await?;
        }
        "minus" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.source)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.target.parse().unwrap();
            let r = left - right;
            inc::set(conn, root, &inc.source, &r.to_string()).await?;
        }
        "mul" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.source)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.target.parse().unwrap();
            let r = left * right;
            inc::set(conn, root, &inc.source, &r.to_string()).await?;
        }
        "div" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.source)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.target.parse().unwrap();
            let r = left / right;
            inc::set(conn, root, &inc.source, &r.to_string()).await?;
        }
        "mod" => {
            let left: i64 = graph::get_or_empty(conn, root, &inc.source)
                .await?
                .parse()
                .unwrap();
            let right: i64 = inc.target.parse().unwrap();
            let r = left % right;
            inc::set(conn, root, &inc.source, &r.to_string()).await?;
        }
        "jump" => {
            let step: i32 = inc.target.parse().unwrap();
            return Ok(InvokeResult::Jump(step));
        }
        _ => todo!()
    }
    Ok(InvokeResult::Jump(1))
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
