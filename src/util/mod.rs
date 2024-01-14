pub mod edge;
pub mod graph;

use sqlx::{MySql, Pool};

pub struct AppState {
    pub pool: Pool<MySql>,
}
