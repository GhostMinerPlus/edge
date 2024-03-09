use std::io;

use sqlx::MySqlConnection;

use crate::mem_table::MemTable;

mod dao;

fn is_temp(source: &str, code: &str, target: &str) -> bool {
    source.starts_with('$') || code.starts_with('$') || target.starts_with('$')
}

async fn commit(dm: &mut DataManager<'_>) -> io::Result<()> {
    dao::insert_edge_mp(dm.conn, &dm.mem_table.take()).await
}

// Public
pub trait AsDataManager: Send {
    /// Insert a new edge
    fn insert_edge(
        &mut self,
        source: &str,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send;

    /// Clear all edge with `source` and `code` and insert a new edge
    fn clear(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// Get a target from `source->code`
    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send;

    /// Get a source from `target<-code`
    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send;

    /// Get all targets from `source->code`
    fn get_target_v(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send;

    /// Get all by path
    fn get_all_by_path(
        &mut self,
        path: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send;

    /// Get all by path
    fn get_one_by_path(
        &mut self,
        path: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send;

    fn get_list(
        &mut self,
        root: &str,
        dimension_v: &Vec<String>,
        attr_v: &Vec<String>,
    ) -> impl std::future::Future<Output = io::Result<json::Array>> + Send;

    async fn commit(&mut self) -> io::Result<()>;

    fn delete_code_without_source(
        &mut self,
        code: &str,
        source_code: &str,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    fn delete_code_without_target(
        &mut self,
        code: &str,
        target_code: &str,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;
}

pub struct DataManager<'a> {
    conn: &'a mut MySqlConnection,
    mem_table: &'a mut MemTable,
}

impl<'a> DataManager<'a> {
    pub fn new(conn: &'a mut MySqlConnection, mem_table: &'a mut MemTable) -> Self {
        Self { conn, mem_table }
    }
}

impl<'a> AsDataManager for DataManager<'a> {
    async fn insert_edge(&mut self, source: &str, code: &str, target: &str) -> io::Result<String> {
        if is_temp(source, code, target) {
            Ok(self.mem_table.insert_temp_edge(source, code, target))
        } else {
            Ok(self.mem_table.insert_edge(source, code, target))
        }
    }

    async fn clear(&mut self, source: &str, code: &str) -> io::Result<()> {
        self.mem_table.delete_edge_with_source_code(source, code);
        dao::delete_edge_with_source_code(&mut self.conn, source, code).await
    }

    async fn get_target(&mut self, source: &str, code: &str) -> io::Result<String> {
        if let Some(target) = self.mem_table.get_target(source, code) {
            return Ok(target);
        } else {
            let (id, target) = dao::get_target(&mut self.conn, source, code).await?;
            self.mem_table
                .append_exists_edge(&id, source, code, &target);
            Ok(target)
        }
    }

    async fn get_source(&mut self, code: &str, target: &str) -> io::Result<String> {
        if let Some(source) = self.mem_table.get_source(code, target) {
            return Ok(source);
        } else {
            let (id, source) = dao::get_source(&mut self.conn, code, target).await?;
            self.mem_table
                .append_exists_edge(&id, &source, code, target);
            Ok(source)
        }
    }

    async fn get_target_v(&mut self, source: &str, code: &str) -> io::Result<Vec<String>> {
        if is_temp(source, code, "") {
            Ok(self.mem_table.get_target_v_unchecked(source, code))
        } else {
            commit(self).await?;
            dao::get_target_v(&mut self.conn, source, code).await
        }
    }

    async fn get_list(
        &mut self,
        root: &str,
        dimension_v: &Vec<String>,
        attr_v: &Vec<String>,
    ) -> io::Result<json::Array> {
        commit(self).await?;
        dao::get_list(&mut self.conn, root, dimension_v, attr_v).await
    }

    async fn commit(&mut self) -> io::Result<()> {
        commit(self).await
    }

    async fn delete_code_without_source(
        &mut self,
        code: &str,
        source_code: &str,
    ) -> io::Result<()> {
        commit(self).await?;
        dao::delete_code_without_source(self.conn, code, source_code).await
    }

    async fn delete_code_without_target(
        &mut self,
        code: &str,
        target_code: &str,
    ) -> io::Result<()> {
        commit(self).await?;
        dao::delete_code_without_target(self.conn, code, target_code).await
    }

    fn get_all_by_path(
        &mut self,
        path: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
        async {
            commit(self).await?;
            dao::get_all_by_path(self.conn, path).await
        }
    }

    fn get_one_by_path(
        &mut self,
        path: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send {
        async {
            commit(self).await?;
            dao::get_one_by_path(self.conn, path).await
        }
    }
}
