use std::io;

use sqlx::MySqlConnection;

use crate::{
    data::DataManager,
    edge::{parse_script, unparse_script, AsEdgeEngine, EdgeEngine},
    mem_table::MemTable,
};

// Public
pub async fn execute(
    conn: &mut MySqlConnection,
    mem_table: &mut MemTable,
    script_vn: &json::JsonValue,
) -> io::Result<json::JsonValue> {
    let dm = DataManager::new(conn, mem_table);
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine.execute(script_vn).await?;
    edge_engine.commit().await?;
    Ok(rs)
}

pub async fn require(
    conn: &mut MySqlConnection,
    mem_table: &mut MemTable,
    target: &str,
    constraint: &str,
) -> io::Result<Vec<String>> {
    let dm = DataManager::new(conn, mem_table);
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .require(&parse_script(target)?, &parse_script(constraint)?)
        .await?;

    edge_engine.commit().await?;
    Ok(rs.into_iter().map(|inc| unparse_script(&inc)).collect())
}
