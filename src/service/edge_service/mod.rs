mod edge;

use std::io;

use sqlx::MySqlConnection;

pub async fn execute(conn: &mut MySqlConnection, script: &str) -> io::Result<String> {
    let root = edge::new_point();
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        let mut word_v: Vec<&str> = line.split(" ").collect();
        while word_v.len() < 3 {
            word_v.push("");
        }
        inc_v.push(edge::Inc {
            code: word_v[0].to_string(),
            input: word_v[1].to_string(),
            output: word_v[2].to_string(),
        });
    }
    edge::invoke_inc_v(conn, &mut root.clone(), &inc_v).await?;
    return Ok(root);
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
                r#"set xxx "->edge->source"
set xxx "->edge->code"
set xxx "->edge->target"
insert ->edge "->id"
delete ->id
return ->id"#,
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
