use std::{io, sync::Arc};

use axum::http::HeaderMap;
use edge_lib::{
    data::{AsDataManager, Auth},
    util::Path,
    EdgeEngine, ScriptTree,
};

use crate::err;

use super::crypto;

// Public
pub async fn register(dm: Arc<dyn AsDataManager>, auth: &crypto::Auth) -> io::Result<()> {
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

pub async fn login(dm: Arc<dyn AsDataManager>, auth: &crypto::Auth) -> io::Result<String> {
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
    crypto::gen_token(&key_v[0], auth.email.clone(), Some(3600))
}

pub async fn execute(
    dm: Arc<dyn AsDataManager>,
    hm: &HeaderMap,
    script_vn: String,
) -> err::Result<String> {
    let cookie = super::get_cookie(hm).map_err(|e| err::Error::NotLogin(e.to_string()))?;
    let (writer, printer) = super::parse_auth(dm.clone(), &cookie)
        .await
        .map_err(|e| err::Error::NotLogin(e.to_string()))?;
    log::info!("email: {}", writer);

    log::info!("executing");
    log::debug!("executing {script_vn}");
    let mut edge_engine = EdgeEngine::new(dm.divide(Auth::writer("", &printer)));
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
    dm: Arc<dyn AsDataManager>,
    hm: &HeaderMap,
    script_vn: String,
) -> err::Result<String> {
    let cookie = super::get_cookie(hm).map_err(|e| err::Error::NotLogin(e.to_string()))?;
    let (writer, printer) = super::parse_auth(dm.clone(), &cookie)
        .await
        .map_err(|e| err::Error::NotLogin(e.to_string()))?;
    log::info!("email: {}", writer);

    log::info!("executing");
    log::debug!("executing {script_vn}");
    let mut edge_engine = EdgeEngine::new(dm.divide(Auth::writer("", &printer)));
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
