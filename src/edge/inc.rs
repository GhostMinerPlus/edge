use std::{cmp::min, io};

use crate::data::AsDataManager;

// Public
pub async fn set(
    _: &mut impl AsDataManager,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
    Ok(input_item_v.into_iter().filter(|s| !s.is_empty()).collect())
}

pub async fn sort(
    dm: &mut impl AsDataManager,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
    let mut temp_item_v = Vec::with_capacity(input_item_v.len());
    for input_item in &input_item_v {
        let no = dm.get_target(input_item, "$no").await?;
        temp_item_v.push((input_item.clone(), no));
    }
    temp_item_v.sort_by(|p, q| p.1.cmp(&q.1));
    let output_item_v = temp_item_v.into_iter().map(|item| item.0).collect();
    Ok(output_item_v)
}

pub async fn add(
    _: &mut impl AsDataManager,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    let sz = min(input_item_v.len(), input1_item_v.len());
    let mut output_item_v = Vec::with_capacity(sz);
    for i in 0..sz {
        let left = input_item_v[i].parse::<f64>();
        if left.is_err() {
            continue;
        }
        let right = input1_item_v[i].parse::<f64>();
        if right.is_err() {
            continue;
        }
        let r: f64 = left.unwrap() + right.unwrap();
        output_item_v.push(r.to_string());
    }
    Ok(output_item_v)
}

pub async fn minus(
    _: &mut impl AsDataManager,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    let sz = min(input_item_v.len(), input1_item_v.len());
    let mut output_item_v = Vec::with_capacity(sz);
    for i in 0..sz {
        let left = input_item_v[i].parse::<f64>();
        if left.is_err() {
            continue;
        }
        let right = input1_item_v[i].parse::<f64>();
        if right.is_err() {
            continue;
        }
        let r: f64 = left.unwrap() - right.unwrap();
        output_item_v.push(r.to_string());
    }
    Ok(output_item_v)
}

pub async fn new(
    _: &mut impl AsDataManager,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    if min(input_item_v.len(), input1_item_v.len()) != 1 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "need 1 but not"));
    }
    let sz = input_item_v[0]
        .parse::<i64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut output_item_v = Vec::with_capacity(sz as usize);
    for _ in 0..sz {
        output_item_v.push(input1_item_v[0].clone());
    }
    Ok(output_item_v)
}

pub async fn append(
    _: &mut impl AsDataManager,
    mut input_item_v: Vec<String>,
    mut input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    input_item_v.append(&mut input1_item_v);
    Ok(input_item_v)
}
