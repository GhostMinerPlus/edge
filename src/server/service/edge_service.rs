use std::io;

use sqlx::MySqlConnection;

use crate::{
    data::DataManager,
    edge::{self, AsEdgeEngine, EdgeEngine},
    mem_table::{new_point, MemTable},
};

pub async fn execute(
    conn: &mut MySqlConnection,
    mem_table: &mut MemTable,
    script: &str,
) -> io::Result<Vec<String>> {
    let mut root = format!("${}", new_point());
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        // <output> <function> <input>
        let word_v: Vec<&str> = line.split(" ").collect();
        match word_v.len() {
            4 => {
                inc_v.push(edge::Inc {
                    output: word_v[0].trim().to_string(),
                    function: word_v[1].trim().to_string(),
                    input: word_v[2].trim().to_string(),
                    input1: word_v[3].trim().to_string(),
                });
            }
            _ => todo!(),
        }
    }
    let dm = DataManager::new(conn, mem_table);
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine.invoke_inc_v(&mut root, &inc_v).await?;
    edge_engine.commit().await?;
    Ok(rs)
}
