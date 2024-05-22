use std::{collections::HashMap, io};

use axum::http::HeaderMap;
use edge_lib::{data::AsDataManager, AsEdgeEngine, EdgeEngine, Path, ScriptTree};

use crate::err;

use super::crypto;

// Public
pub fn get_cookie(hm: &HeaderMap) -> err::Result<HashMap<String, String>> {
    let cookie: &str = match hm.get("Cookie") {
        Some(r) => match r.to_str() {
            Ok(r) => r,
            Err(e) => {
                return Err(err::Error::Other(e.to_string()));
            }
        },
        None => {
            return Err(err::Error::Other(format!("no cookie")));
        }
    };
    let pair_v: Vec<Vec<&str>> = cookie
        .split(';')
        .into_iter()
        .map(|pair| pair.split('=').collect::<Vec<&str>>())
        .collect();
    let mut cookie = HashMap::with_capacity(pair_v.len());
    for pair in pair_v {
        if pair.len() != 2 {
            continue;
        }
        cookie.insert(pair[0].to_string(), pair[1].to_string());
    }
    Ok(cookie)
}

pub async fn parse_auth(
    dm: &mut dyn AsDataManager,
    cookie: &HashMap<String, String>,
) -> err::Result<crypto::User> {
    let token = match cookie.get("token") {
        Some(r) => r,
        None => {
            return Err(err::Error::Other("no token".to_lowercase()));
        }
    };
    let key = dm
        .get(&Path::from_str("root->key"))
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    if key.is_empty() {
        return Err(err::Error::Other("no key".to_string()));
    }
    crypto::parse_token(&key[0], token)
}

pub async fn register(mut dm: Box<dyn AsDataManager>, auth: &crypto::Auth) -> io::Result<()> {
    let key_v = dm.get(&Path::from_str("root->key")).await?;
    if key_v.is_empty() {
        return Err(io::Error::other("no key"));
    }

    if !dm
        .get(&Path::from_str(&format!("{}<-email", auth.email)))
        .await?
        .is_empty()
    {
        return Err(io::Error::other("user already exists"));
    }
    let mut edge_engine = EdgeEngine::new(dm);
    edge_engine
        .execute1(&ScriptTree {
            script: [
                format!("$->$user = = ? _"),
                format!("$->$user->email = = {} _", auth.email),
                format!("$->$user->password = = {} _", auth.password),
                format!("root->user += = $->$user _"),
            ]
            .join("\n"),
            name: format!("result"),
            next_v: vec![],
        })
        .await?;
    edge_engine.commit().await?;
    Ok(())
}

pub async fn login(mut dm: Box<dyn AsDataManager>, auth: &crypto::Auth) -> io::Result<String> {
    let key_v = dm.get(&Path::from_str("root->key")).await?;
    if key_v.is_empty() {
        return Err(io::Error::other("no key"));
    }

    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .execute1(&ScriptTree {
            script: [format!(
                "$->$output = inner {}<-email {}<-password",
                auth.email, auth.password
            )]
            .join("\n"),
            name: format!("result"),
            next_v: vec![],
        })
        .await?;
    if rs["result"].is_empty() {
        return Err(io::Error::other("user not exists"));
    }
    crypto::gen_token(&key_v[0], auth)
}

pub async fn execute(
    mut dm: Box<dyn AsDataManager>,
    hm: &HeaderMap,
    script_vn: String,
) -> err::Result<String> {
    let cookie = get_cookie(hm).map_err(|e| err::Error::NotLogin(e.to_string()))?;
    let auth = parse_auth(&mut *dm, &cookie)
        .await
        .map_err(|e| err::Error::NotLogin(e.to_string()))?;
    log::info!("email: {}", auth.email);

    log::info!("executing");
    log::debug!("executing {script_vn}");
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .execute(&json::parse(&script_vn).unwrap())
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    log::info!("commited");
    Ok(rs.dump())
}

pub async fn execute1(
    mut dm: Box<dyn AsDataManager>,
    hm: &HeaderMap,
    script_vn: String,
) -> err::Result<String> {
    let cookie = get_cookie(hm).map_err(|e| err::Error::NotLogin(e.to_string()))?;
    let auth = parse_auth(&mut *dm, &cookie)
        .await
        .map_err(|e| err::Error::NotLogin(e.to_string()))?;
    log::info!("email: {}", auth.email);

    log::info!("executing");
    log::debug!("executing {script_vn}");
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .execute1(&serde_json::from_str(&script_vn).unwrap())
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    log::info!("commited");
    Ok(rs.dump())
}
