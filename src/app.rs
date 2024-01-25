use sqlx::{MySql, Pool};
use tokio::sync::Mutex;

use crate::mem_table::MemTable;

pub struct AppState {
    pub pool: Pool<MySql>,
    pub mem_table: Mutex<MemTable>,
}
