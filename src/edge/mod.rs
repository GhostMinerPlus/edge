mod inc;

use serde::Deserialize;
use std::io;

use crate::data::DataManager;

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

pub async fn invoke_inc(
    dm: &mut DataManager<'_>,
    root: &mut String,
    inc: &Inc,
) -> io::Result<InvokeResult> {
    match inc.code.as_str() {
        "return" => Ok(InvokeResult::Return(inc.target.clone())),
        "dump" => Ok(InvokeResult::Return(inc::dump(dm, &inc.target).await?)),
        "asign" => {
            inc::asign(dm, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "delete" => {
            inc::delete(dm, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "set" => {
            inc::set(dm, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "append" => {
            inc::append(dm, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        _ => todo!(),
    }
}

pub async fn unwrap_inc(
    dm: &mut DataManager<'_>,
    root: &str,
    inc: &Inc,
) -> io::Result<Inc> {
    Ok(Inc {
        source: inc::unwrap_value(dm, root, &inc.source).await?,
        code: inc::unwrap_value(dm, root, &inc.code).await?,
        target: inc::unwrap_value(dm, root, &inc.target).await?,
    })
}

pub async fn invoke_inc_v(
    dm: &mut DataManager<'_>,
    root: &mut String,
    inc_v: &Vec<Inc>,
) -> io::Result<String> {
    let mut pos = 0i32;
    while (pos as usize) < inc_v.len() {
        let inc = unwrap_inc(dm, &root, &inc_v[pos as usize]).await?;
        match invoke_inc(dm, root, &inc).await? {
            InvokeResult::Jump(step) => pos += step,
            InvokeResult::Return(s) => {
                return Ok(s);
            }
        }
    }
    Ok(String::new())
}
