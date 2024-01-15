mod inc;

use serde::Deserialize;
use sqlx::MySqlConnection;
use std::io;

use crate::util::graph;

use super::graph::{insert_edge, new_point, get_list};

async fn invoke_script(
    conn: &mut MySqlConnection,
    root: &mut String,
    mut inc_v_h: String,
) -> io::Result<String> {
    let mut inc_v = Vec::new();
    loop {
        inc_v_h = match graph::get_object(conn, &inc_v_h, "next").await {
            Ok(r) => r,
            Err(_) => break,
        };
        let subject = graph::get_object(conn, &inc_v_h, "subject").await?;
        let predicate = graph::get_object(conn, &inc_v_h, "predicate").await?;
        let object = graph::get_object(conn, &inc_v_h, "object").await?;
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

async fn get_arr(
    conn: &mut MySqlConnection,
    object: &str,
    attr_v: &Vec<String>,
) -> io::Result<json::Array> {
    let mut arr = json::Array::new();
    let mut iter = graph::get_object_or_empty(conn, object, "first").await?;
    while !iter.is_empty() {
        let mut item = json::object! {};
        for attr in attr_v {
            item[attr] =
                json::JsonValue::String(graph::get_object_or_empty(conn, &iter, attr).await?);
        }
        arr.push(item);
        iter = graph::get_object_or_empty(conn, &iter, "next").await?;
    }
    return Ok(arr);
}

// Public
#[derive(Clone, Deserialize)]
pub struct Inc {
    pub subject: String,
    pub predicate: String,
    pub object: String,
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
    let (handler, class) = if let Ok(class) = graph::get_object(conn, &inc.predicate, "class").await
    {
        (inc.predicate.as_str(), class)
    } else {
        ("", inc.predicate.clone())
    };
    match class.as_str() {
        "return" => {
            if handler.is_empty() {
                return Ok(InvokeResult::Return(inc.object.clone()));
            }
            if let Ok(_) = graph::get_object(conn, handler, "json").await {
                // "" return --json huiwen->canvas->edge_v --dimension 2 --attr pos --attr color --attr width
                let object = graph::get_object(conn, &inc.object, "class").await?;
                let dimension = graph::get_object(conn, &inc.object, "dimension")
                    .await?
                    .parse::<i32>()
                    .unwrap();
                let attr_v = graph::get_object_v(conn, &inc.object, "attr").await?;
                if dimension == 1 {
                    let arr = get_arr(conn, &object, &attr_v).await?;
                    return Ok(InvokeResult::Return(json::stringify(arr)));
                } else if dimension == 2 {
                    let mut arr = json::Array::new();
                    let mut iter = graph::get_object_or_empty(conn, &object, "first").await?;
                    while !iter.is_empty() {
                        let first = graph::get_object_or_empty(conn, &iter, "first").await?;
                        arr.push(json::JsonValue::Array(get_list(conn, &first, &attr_v).await?));
                        iter = graph::get_object_or_empty(conn, &iter, "next").await?;
                    }
                    return Ok(InvokeResult::Return(json::stringify(arr)));
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported dimension",
                    ));
                }
            }
        }
        "set" => {
            inc::set(conn, &root, &inc.subject, &inc.object).await?;
        }
        "append" => {
            inc::append(conn, &root, &inc.subject, &inc.object).await?;
        }
        "delete" => {
            inc::delete_edge(conn, &inc.object).await?;
        }
        "insert" => {
            let subject = graph::get_object(conn, &inc.object, "subject").await?;
            let predicate = graph::get_object(conn, &inc.object, "predicate").await?;
            let object = graph::get_object(conn, &inc.object, "object").await?;
            let id = graph::insert_edge(conn, &subject, &predicate, &object).await?;
            inc::set(conn, root, &inc.subject, &id).await?;
        }
        "cmp" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.subject)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.object.parse().unwrap();
            let r = if left < right {
                "1"
            } else if left > right {
                "3"
            } else {
                "2"
            };
            inc::set(conn, root, &inc.subject, r).await?;
        }
        "cmp_str" => {
            let left = graph::get_or_empty(conn, root, &inc.subject).await?;
            let right = &inc.object;
            let r = if &left == right { "1" } else { "2" };
            inc::set(conn, root, &inc.subject, r).await?;
        }
        "add" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.subject)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.object.parse().unwrap();
            let r = left + right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "minus" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.subject)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.object.parse().unwrap();
            let r = left - right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "mul" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.subject)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.object.parse().unwrap();
            let r = left * right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "div" => {
            let left: f64 = graph::get_or_empty(conn, root, &inc.subject)
                .await?
                .parse()
                .unwrap();
            let right: f64 = inc.object.parse().unwrap();
            let r = left / right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "mod" => {
            let left: i64 = graph::get_or_empty(conn, root, &inc.subject)
                .await?
                .parse()
                .unwrap();
            let right: i64 = inc.object.parse().unwrap();
            let r = left % right;
            inc::set(conn, root, &inc.subject, &r.to_string()).await?;
        }
        "jump" => {
            let step: i32 = inc.object.parse().unwrap();
            return Ok(InvokeResult::Jump(step));
        }
        _ => {
            // Not a atomic predicate
            let mut new_root = new_point();
            insert_edge(conn, &new_root, "subject", &inc.subject).await?;
            insert_edge(conn, &new_root, "object", &inc.object).await?;
            let r = invoke_script(conn, &mut new_root, inc.predicate.clone()).await?;
            inc::set(conn, root, &inc.subject, &r).await?;
        }
    }
    Ok(InvokeResult::Jump(1))
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
        match invoke_inc(conn, root, &inc).await? {
            InvokeResult::Jump(step) => pos += step,
            InvokeResult::Return(s) => {
                return Ok(s);
            }
        }
    }
    Ok(String::new())
}
