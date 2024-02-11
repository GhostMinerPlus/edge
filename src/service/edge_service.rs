use std::io;

use sqlx::MySqlConnection;

use crate::{
    data::{AsDataManager, DataManager},
    edge,
    mem_table::{new_point, MemTable},
};

pub async fn execute(
    conn: &mut MySqlConnection,
    mem_table: &mut MemTable,
    script: &str,
) -> io::Result<String> {
    let mut root = format!("${}", new_point());
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        // <source> <code> <target>
        let word_v: Vec<&str> = line.split(" ").collect();
        match word_v.len() {
            3 => {
                inc_v.push(edge::Inc {
                    source: word_v[0].trim().to_string(),
                    code: word_v[1].trim().to_string(),
                    target: word_v[2].trim().to_string(),
                });
            }
            _ => todo!(),
        }
    }
    let mut dm = DataManager::new(conn, mem_table);
    let rs = edge::invoke_inc_v(&mut dm, &mut root, &inc_v).await?;
    dm.commit().await?;
    Ok(rs)
}

#[cfg(test)]
mod tests {
    use earth::AsConfig;
    use sqlx::{Acquire, MySql, Pool};

    use crate::{mem_table::MemTable, Config};

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
            let mut mem_table = MemTable::new();

            let mut tr = pool.begin().await.unwrap();
            let mut conn = tr.acquire().await.unwrap();
            let r = super::execute(
                &mut conn,
                &mut mem_table,
                r#""->result->root" set bf9e7faa-435f-4234-9e22-4db368a80396
"->result->dimension" set edge
"->result->dimension" append point
"->result->attr" set pos
"->result->attr" append color
"->result->attr" append width
"" dump ->result"#,
            )
            .await;
            let _ = tr.rollback().await;
            // tr.rollback().await.unwrap();
            println!("{}", r.unwrap());
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }
}
