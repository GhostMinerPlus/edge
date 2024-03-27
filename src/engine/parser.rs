use crate::{
    data::AsDataManager,
    err::{Error, ErrorKind, Result},
};

use super::{inc, util};

// Public
pub fn unwrap_value(root: &str, value: &str) -> String {
    if value == "?" {
        uuid::Uuid::new_v4().to_string()
    } else if value == "$" {
        root.to_string()
    } else if value == "_" {
        "".to_string()
    } else if value.starts_with("$<-") {
        format!("{root}{}", &value[1..])
    } else if value.starts_with("$->") {
        format!("{root}{}", &value[1..])
    } else {
        value.to_string()
    }
}

pub async fn unwrap_inc(
    dm: &mut impl AsDataManager,
    root: &str,
    inc: &inc::Inc,
) -> Result<inc::Inc> {
    let inc = inc::Inc {
        output: unwrap_value(root, &inc.output),
        operator: util::get_one(dm, root, &inc.operator).await?,
        function: util::get_one(dm, root, &inc.function).await?,
        input: unwrap_value(root, &inc.input),
        input1: unwrap_value(root, &inc.input1),
    };
    Ok(inc)
}

pub fn parse_script(script: &str) -> Result<Vec<inc::Inc>> {
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        // <output> <operator> <function> <input>
        let word_v: Vec<&str> = line.split(" ").collect();
        if word_v.len() != 5 {
            log::error!("while parsing script: word_v.len() != 5");
            return Err(Error::new(
                ErrorKind::InvalidScript,
                "while parsing script".to_string(),
            ));
        }
        inc_v.push(inc::Inc {
            output: word_v[0].trim().to_string(),
            operator: word_v[1].trim().to_string(),
            function: word_v[2].trim().to_string(),
            input: word_v[3].trim().to_string(),
            input1: word_v[4].trim().to_string(),
        });
    }
    Ok(inc_v)
}

pub fn unparse_script(inc_v: &Vec<inc::Inc>) -> String {
    inc_v
        .into_iter()
        .map(inc::Inc::to_string)
        .reduce(|acc, item| format!("{acc}\n{item}"))
        .unwrap()
}
