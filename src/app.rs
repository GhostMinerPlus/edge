use edge_lib::mem_table::MemTable;
use sqlx::{MySql, Pool};
use tokio::sync::Mutex;

pub struct AppState {
    pub pool: Pool<MySql>,
    pub mem_table: Mutex<MemTable>,
}
