pub mod edge;
pub mod graph;

use sqlx::{MySql, Pool};

pub struct AppState {
    pub pool: Pool<MySql>,
}

pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}
