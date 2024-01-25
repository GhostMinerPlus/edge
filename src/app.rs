use sqlx::{MySql, Pool};

pub struct AppState {
    pub pool: Pool<MySql>,
}
