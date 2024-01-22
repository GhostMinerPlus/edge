use std::io;

use sqlx::MySqlConnection;

use crate::util;

pub async fn execute(conn: &mut MySqlConnection, script: &str) -> io::Result<String> {
    let mut root = util::graph::new_point();
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        // <source> <code> <target>
        let word_v: Vec<&str> = line.split(" ").collect();
        match word_v.len() {
            3 => {
                inc_v.push(util::edge::Inc {
                    source: word_v[0].trim().to_string(),
                    code: word_v[1].trim().to_string(),
                    target: word_v[2].trim().to_string(),
                });
            }
            _ => todo!(),
        }
    }
    util::edge::invoke_inc_v(conn, &mut root, &inc_v).await
}

#[cfg(test)]
mod tests {
    use earth::AsConfig;
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
        let mut config = Config::default();
        config.merge_by_file("config.toml");
        let f = async {
            let pool: Pool<MySql> = sqlx::Pool::connect(&config.db_url).await.unwrap();

            let mut tr = pool.begin().await.unwrap();
            let mut conn = tr.acquire().await.unwrap();
            let r = super::execute(
                &mut conn,
                r#""huiwen->version" set v0.1.0
huiwen new canvas
"" return huiwen->canvas"#,
            )
            .await;
            tr.rollback().await.unwrap();
            println!("{}", r.unwrap());
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }
}
