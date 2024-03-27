use sqlx::MySqlConnection;

use crate::{
    data::mem_table::MemTable,
    data::DataManager,
    engine::{parser, AsEdgeEngine, EdgeEngine},
    err::Result,
};

// Public
pub async fn execute(
    conn: &mut MySqlConnection,
    mem_table: &mut MemTable,
    script_vn: &json::JsonValue,
) -> Result<json::JsonValue> {
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
) -> Result<Vec<String>> {
    let dm = DataManager::new(conn, mem_table);
    let mut edge_engine = EdgeEngine::new(dm);
    let rs = edge_engine
        .require(
            &parser::parse_script(target)?,
            &parser::parse_script(constraint)?,
        )
        .await?;

    edge_engine.commit().await?;
    Ok(rs
        .into_iter()
        .map(|inc| parser::unparse_script(&inc))
        .collect())
}
