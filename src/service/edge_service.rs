use std::io;

use sqlx::MySqlConnection;

use crate::util;

pub async fn execute(conn: &mut MySqlConnection, script: &str) -> io::Result<String> {
    let mut root = "root".to_string();
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        // [output = ]<subject> <predicate> <object>
        let pair: Vec<&str> = line.split("=").collect();
        match pair.len() {
            1 => {
                let word_v: Vec<&str> = pair[0].split(" ").collect();
                inc_v.push(util::edge::Inc {
                    subject: word_v[0].trim().to_string(),
                    predicate: word_v[1].trim().to_string(),
                    object: word_v[2].trim().to_string(),
                    output: "".to_string(),
                });
            }
            2 => {
                let word_v: Vec<&str> = pair[1].split(" ").collect();
                inc_v.push(util::edge::Inc {
                    subject: word_v[0].trim().to_string(),
                    predicate: word_v[1].trim().to_string(),
                    object: word_v[2].trim().to_string(),
                    output: pair[0].trim().to_string(),
                });
            }
            _ => todo!(),
        }
    }
    util::edge::invoke_inc_v(conn, &mut root, &inc_v).await
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sqlx::{Acquire, MySql, Pool};

    use crate::Config;

    fn init() {
        let _ =
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("DEBUG"))
                .is_test(true)
                .try_init();
    }

    #[test]
    fn test_execute() {
        init();
        let f = async {
            let config: Config =
                toml::from_str(&fs::read_to_string("config.toml").unwrap()).unwrap();
            let pool: Pool<MySql> = sqlx::Pool::connect(&config.db_url).await.unwrap();

            let mut tr = pool.begin().await.unwrap();
            let mut conn = tr.acquire().await.unwrap();
            let r = super::execute(
                &mut conn,
                r#"set xxx "edge->source"
set xxx "edge->code"
set xxx "edge->target"
insert edge "edge->id"
delete edge->id
return edge->id"#,
            )
            .await;
            tr.rollback().await.unwrap();
            assert!(!r.unwrap().is_empty());
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }
}
