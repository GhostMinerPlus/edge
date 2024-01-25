use std::io;

use sqlx::MySqlConnection;

use crate::mem_table::MemTable;

mod dao;

// Public
pub struct DataManager<'a> {
    conn: &'a mut MySqlConnection,
    mem_table: &'a mut MemTable,
}

impl<'a> DataManager<'a> {
    pub fn new(conn: &'a mut MySqlConnection, mem_table: &'a mut MemTable) -> Self {
        Self { conn, mem_table }
    }

    pub async fn insert_edge(
        &mut self,
        source: &str,
        code: &str,
        no: u64,
        target: &str,
    ) -> io::Result<String> {
        dao::insert_edge(&mut self.conn, source, code, no, target).await
    }

    pub async fn get_target(&mut self, source: &str, code: &str) -> io::Result<String> {
        dao::get_target(&mut self.conn, source, code).await
    }

    pub async fn get_target_v(&mut self, source: &str, code: &str) -> io::Result<Vec<String>> {
        dao::get_target_v(&mut self.conn, source, code).await
    }

    pub async fn get_source(&mut self, code: &str, target: &str) -> io::Result<String> {
        dao::get_source(&mut self.conn, code, target).await
    }

    pub async fn set_target(
        &mut self,
        source: &str,
        code: &str,
        target: &str,
    ) -> io::Result<String> {
        dao::set_target(&mut self.conn, source, code, target).await
    }

    pub async fn append_target(
        &mut self,
        source: &str,
        code: &str,
        target: &str,
    ) -> io::Result<String> {
        dao::append_target(&mut self.conn, source, code, target).await
    }

    pub async fn get_list(
        &mut self,
        root: &str,
        dimension_v: &Vec<String>,
        attr_v: &Vec<String>,
    ) -> io::Result<json::Array> {
        dao::get_list(&mut self.conn, root, dimension_v, attr_v).await
    }
}
