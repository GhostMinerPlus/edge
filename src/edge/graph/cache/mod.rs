use std::io;

use sqlx::MySqlConnection;

mod dao;

// Public
pub async fn insert_edge(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    no: u64,
    target: &str,
) -> io::Result<String> {
    dao::insert_edge(conn, source, code, no, target).await
}

pub async fn get_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<String> {
    dao::get_target(conn, source, code).await
}

pub async fn get_target_v(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
) -> io::Result<Vec<String>> {
    dao::get_target_v(conn, source, code).await
}

pub async fn get_source(
    conn: &mut MySqlConnection,
    code: &str,
    target: &str,
) -> io::Result<String> {
    dao::get_source(conn, code, target).await
}

pub async fn set_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    dao::set_target(conn, source, code, target).await
}

pub async fn append_target(
    conn: &mut MySqlConnection,
    source: &str,
    code: &str,
    target: &str,
) -> io::Result<String> {
    dao::append_target(conn, source, code, target).await
}

pub async fn get_list(
    conn: &mut MySqlConnection,
    root: &str,
    dimension_v: &Vec<String>,
    attr_v: &Vec<String>,
) -> io::Result<json::Array> {
    dao::get_list(conn, root, dimension_v, attr_v).await
}
