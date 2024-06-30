use std::{io, sync::Arc};

use edge_lib::{
    data::{AsDataManager, Auth},
    util::Path,
    EdgeEngine, ScriptTree,
};

use crate::err;

use super::{crypto, Paper};

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
    edge_engine.commit().await?;
    if rs["result"].is_empty() {
        return Err(io::Error::other("user not exists"));
    }
    crypto::gen_token(&key_v[0], auth.email.clone(), Some(3600))
}

pub async fn execute(
    dm: Arc<dyn AsDataManager>,
    writer: String,
    paper: String,
    pen: String,
    script_vn: &json::JsonValue,
) -> err::Result<String> {
    log::info!("executing");
    if !is_writer_or_higher(&dm, &writer, &paper).await? {
        return Err(err::Error::Other(
            "you can not write in this paper".to_string(),
        ));
    }
    log::debug!("executing {script_vn}");
    let mut edge_engine = EdgeEngine::new(dm.divide(Auth::writer(&paper, &pen)));
    let rs = edge_engine
        .execute(script_vn)
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
    writer: String,
    paper: String,
    pen: String,
    script_vn: &ScriptTree,
) -> err::Result<String> {
    log::info!("executing");
    if !is_writer_or_higher(&dm, &writer, &paper).await? {
        return Err(err::Error::Other(
            "you can not write in this paper".to_string(),
        ));
    }
    let mut edge_engine = EdgeEngine::new(dm.divide(Auth::writer(&paper, &pen)));
    let rs = edge_engine
        .execute1(&script_vn)
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    log::info!("commited");
    Ok(rs.dump())
}

pub async fn put_paper(
    dm: Arc<dyn AsDataManager>,
    writer: String,
    paper: Paper,
) -> err::Result<String> {
    log::info!("put_paper");
    let mut edge_engine = EdgeEngine::new(dm.clone());
    let rs = edge_engine
        .execute1(&ScriptTree {
            script: [
                format!("$->$paper = ? _"),
                format!("$->$paper->name = {} _", paper.name),
                format!("{writer}->paper append {writer}->paper $->$paper"),
                "$->$output = $->$paper _".to_string(),
            ]
            .join("\n"),
            name: "result".to_string(),
            next_v: vec![],
        })
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    let paper_id = rs["result"][0].as_str().unwrap().to_string();
    dm.set(
        &Path::from_str(&format!("{paper_id}->writer")),
        paper.writer_v,
    )
    .await
    .map_err(|e| err::Error::Other(e.to_string()))?;
    dm.set(
        &Path::from_str(&format!("{paper_id}->manager")),
        paper.manager_v,
    )
    .await
    .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    log::info!("commited");
    Ok(paper_id)
}

pub async fn delete_paper(
    dm: Arc<dyn AsDataManager>,
    writer: String,
    paper: String,
) -> err::Result<()> {
    log::info!("delete_paper");
    if !is_owner(&dm, &writer, &paper).await? {
        return Err(err::Error::Other(
            "you can not delete this paper".to_string(),
        ));
    }
    let mut edge_engine = EdgeEngine::new(dm);
    edge_engine
        .execute1(&ScriptTree {
            script: [
                format!("{paper}->writer = _ _"),
                format!("{paper}->manager = _ _"),
                format!("{writer}->paper left {writer}->paper {paper}"),
            ]
            .join("\n"),
            name: "result".to_string(),
            next_v: vec![],
        })
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(())
}

pub async fn get_paper(dm: Arc<dyn AsDataManager>, writer: String) -> err::Result<String> {
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .execute1(&ScriptTree {
            script: format!("$->$output = {writer}->paper _"),
            name: "paper".to_string(),
            next_v: vec![
                ScriptTree {
                    script: format!("$->$output = $->$input _"),
                    name: "id".to_string(),
                    next_v: vec![],
                },
                ScriptTree {
                    script: format!("$->$output = $->$input->name _"),
                    name: "name".to_string(),
                    next_v: vec![],
                },
            ],
        })
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(rs.dump())
}

pub async fn get_paper_writer(
    dm: Arc<dyn AsDataManager>,
    writer: String,
    paper_id: String,
) -> err::Result<String> {
    if !is_writer_or_higher(&dm, &writer, &paper_id).await? {
        return Err(err::Error::Other("you can not read this paper".to_string()));
    }
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .execute1(&ScriptTree {
            script: format!("$->$output = {paper_id}->writer _"),
            name: "writer".to_string(),
            next_v: vec![],
        })
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(rs.dump())
}

pub async fn update_paper(
    dm: Arc<dyn AsDataManager>,
    writer: String,
    paper: Paper,
) -> err::Result<()> {
    let mut edge_engine = EdgeEngine::new(dm.clone());
    if is_owner(&dm, &writer, &paper.paper_id).await? {
        edge_engine
            .execute1(&ScriptTree {
                script: format!("{}->name = {} _", paper.paper_id, paper.name),
                name: "result".to_string(),
                next_v: vec![],
            })
            .await
            .map_err(|e| err::Error::Other(e.to_string()))?;
        dm.set(
            &Path::from_str(&format!("{}->manager", paper.paper_id)),
            paper.manager_v,
        )
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
        dm.set(
            &Path::from_str(&format!("{}->writer", paper.paper_id)),
            paper.writer_v,
        )
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    } else if is_manager(&dm, &writer, &paper.paper_id).await? {
        dm.set(
            &Path::from_str(&format!("{}->writer", paper.paper_id)),
            paper.writer_v,
        )
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    } else {
        return Err(err::Error::Other(
            "you can not update this paper".to_string(),
        ));
    }
    edge_engine
        .commit()
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(())
}

async fn is_writer_or_higher(
    dm: &Arc<dyn AsDataManager>,
    writer: &String,
    paper_id: &str,
) -> err::Result<bool> {
    if is_writer(dm, writer, paper_id).await? {
        return Ok(true);
    }
    is_manager_or_higher(dm, writer, paper_id).await
}

async fn is_manager_or_higher(
    dm: &Arc<dyn AsDataManager>,
    writer: &String,
    paper_id: &str,
) -> err::Result<bool> {
    if is_manager(dm, writer, paper_id).await? {
        return Ok(true);
    }
    is_owner(dm, writer, paper_id).await
}

async fn is_writer(
    dm: &Arc<dyn AsDataManager>,
    writer: &String,
    paper_id: &str,
) -> err::Result<bool> {
    if paper_id.is_empty() {
        return Ok(true);
    }
    let writer_v = dm
        .get(&Path::from_str(&format!("{paper_id}->writer")))
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(writer_v.contains(writer))
}

async fn is_manager(
    dm: &Arc<dyn AsDataManager>,
    writer: &String,
    paper_id: &str,
) -> err::Result<bool> {
    if paper_id.is_empty() {
        return Ok(false);
    }
    let manager_v = dm
        .get(&Path::from_str(&format!("{paper_id}->manager")))
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(manager_v.contains(writer))
}

async fn is_owner(
    dm: &Arc<dyn AsDataManager>,
    writer: &String,
    paper_id: &str,
) -> err::Result<bool> {
    if paper_id.is_empty() {
        return Ok(false);
    }
    let owner_v = dm
        .get(&Path::from_str(&format!("{paper_id}<-paper")))
        .await
        .map_err(|e| err::Error::Other(e.to_string()))?;
    Ok(owner_v.contains(writer))
}
