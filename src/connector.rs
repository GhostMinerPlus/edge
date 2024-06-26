use std::{io, sync::Arc, time::Duration};

use edge_lib::{data::AsDataManager, EdgeEngine, ScriptTree};
use tokio::time;

use crate::util;

pub struct HttpConnector {
    dm: Arc<dyn AsDataManager>,
}

impl HttpConnector {
    pub fn new(dm: Arc<dyn AsDataManager>) -> Self {
        Self { dm }
    }

    pub async fn run(self) -> io::Result<()> {
        loop {
            if let Err(e) = self.execute().await {
                log::warn!("{e}\nwhen run");
            }

            time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn execute(&self) -> io::Result<()> {
        let ip = util::native::get_global_ipv6()
            .map_err(|e| io::Error::other(format!("{e}\nwhen execute")))?;

        let mut edge_engine = EdgeEngine::new(self.dm.clone());
        let rs = edge_engine
            .execute1(&ScriptTree {
                script: [
                    "$->$output = = root->name _",
                    "$->$output append $->$output root->port",
                    "$->$output append $->$output root->path",
                ]
                .join("\n"),
                name: format!("info"),
                next_v: vec![],
            })
            .await
            .map_err(|e| io::Error::other(format!("{e}\nwhen execute")))?;
        log::debug!("{rs}");
        let name = rs["info"][0].as_str().unwrap();
        let port = rs["info"][1].as_str().unwrap();
        let path = rs["info"][2].as_str().unwrap();

        let rs = edge_engine
            .execute1(&ScriptTree {
                script: ["$->$output = = root->moon_server _"].join("\n"),
                name: format!("moon_server"),
                next_v: vec![],
            })
            .await
            .map_err(|e| io::Error::other(format!("{e}\nwhen execute")))?;
        log::debug!("{rs}");
        let moon_server_v = &rs["moon_server"];

        let script = [
            &format!("$->$server_exists = inner root->web_server {name}<-name"),
            "$->$web_server = if $->$server_exists ?",
            &format!("$->$web_server->name = = {name} _"),
            &format!("$->$web_server->ip = = {ip} _"),
            &format!("$->$web_server->port = = {port} _"),
            &format!("$->$web_server->path = = {path} _"),
            "root->web_server += left $->$web_server $->$server_exists",
        ]
        .join("\\n");
        for moon_server in moon_server_v.members() {
            let uri = match moon_server.as_str() {
                Some(uri) => uri,
                None => {
                    log::error!("failed to parse uri for moon_server\nwhen execute");
                    continue;
                }
            };
            log::info!("reporting to {uri}");
            if let Err(e) = util::http_execute(&uri, format!("{{\"{script}\": null}}")).await {
                log::warn!("{e}\nwhen execute");
                if let Err(e) = util::http_execute1(
                    &uri,
                    &ScriptTree {
                        script: script.replace("\\n", "\n"),
                        name: format!("info"),
                        next_v: vec![],
                    },
                )
                .await
                {
                    log::warn!("{e}\nwhen execute");
                }
            } else {
                log::info!("reported to {uri}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use edge_lib::{
        data::{AsDataManager, Auth, MemDataManager},
        util::Path,
        EdgeEngine, ScriptTree,
    };

    #[test]
    fn test() {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let dm = Arc::new(MemDataManager::new(Auth::printer("root")));
                let mut edge_engine = EdgeEngine::new(dm.clone());
                // config.ip, config.port, config.name
                let name = "test";
                let ip = "0.0.0.0";
                let port = "8080";
                let path = "/test";
                let script = [
                    &format!("$->$server_exists = inner root->web_server {name}<-name"),
                    "$->$web_server = if $->$server_exists ?",
                    &format!("$->$web_server->name = = {name} _"),
                    &format!("$->$web_server->ip = = {ip} _"),
                    &format!("$->$web_server->port = = {port} _"),
                    &format!("$->$web_server->path = = {path} _"),
                    "root->web_server += left $->$web_server $->$server_exists",
                ]
                .join("\n");
                edge_engine
                    .execute1(&ScriptTree {
                        script,
                        name: format!("info"),
                        next_v: vec![],
                    })
                    .await
                    .unwrap();
                edge_engine.commit().await.unwrap();
                let rs = dm.get(&Path::from_str("root->web_server")).await.unwrap();
                assert!(!rs.is_empty());
            })
    }
}
