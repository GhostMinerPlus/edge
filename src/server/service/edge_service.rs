use std::io;

use sqlx::MySqlConnection;

use crate::{
    data::DataManager,
    edge::{AsEdgeEngine, EdgeEngine},
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
