use std::{io, time::Duration};

use earth::AsConfig;
use edge::{data::DataManager, server};
use edge_lib::{data::AsDataManager, AsEdgeEngine, EdgeEngine, ScriptTree};
use serde::{Deserialize, Serialize};
use tokio::time;

#[derive(Debug, Deserialize, Serialize, Clone, AsConfig)]
struct Config {
    ip: String,
    name: String,
    port: u16,
    db_url: String,
    thread_num: u8,
    log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ip: "0.0.0.0".to_string(),
            name: "edge".to_string(),
            port: 80,
            db_url: Default::default(),
            thread_num: 8,
            log_level: "INFO".to_string(),
        }
    }
}

fn main() -> io::Result<()> {
    let mut arg_v: Vec<String> = std::env::args().collect();
    arg_v.remove(0);
    let file_name = if !arg_v.is_empty() && !arg_v[0].starts_with("--") {
        arg_v.remove(0)
    } else {
        "config.toml".to_string()
    };

    let mut config = Config::default();
    config.merge_by_file(&file_name);
    if !arg_v.is_empty() {
        config.merge_by_arg_v(&arg_v);
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log_level))
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.thread_num as usize)
        .build()?
        .block_on(async {
            let pool = sqlx::Pool::connect(&config.db_url)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let dm = DataManager::new(pool);
            let mut edge_engine = EdgeEngine::new(dm.divide());
            // config.ip, config.port, config.name
            edge_engine
                .execute(&ScriptTree {
                    script: [
                        format!("root->name = = {} _", config.name),
                        format!("root->ip = = {} _", config.ip),
                        format!("root->port = = {} _", config.port),
                    ]
                    .join("\n"),
                    name: "".to_string(),
                    next_v: vec![],
                })
                .await?;
            edge_engine.commit().await?;

            tokio::spawn(server::HttpServer::new(dm.divide()).run());
            loop {
                log::info!("alive");
                time::sleep(Duration::from_secs(10)).await;
            }
        })
}
