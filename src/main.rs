use earth::AsConfig;
use err::ErrorKind;
use serde::{Deserialize, Serialize};

mod data;
mod engine;
mod err;
mod server;

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

fn make_config() -> Config {
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
    config
}

fn main() -> err::Result<()> {
    let config = make_config();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log_level))
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.thread_num as usize)
        .build()
        .map_err(|e| err::Error::new(ErrorKind::Other, e.to_string()))?
        .block_on(server::Server::new(config.ip, config.port, config.name, config.db_url).run())
        .map_err(|e| err::Error::new(ErrorKind::Other, e.to_string()))
}
