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
pub struct Edge {
    pub source: String,
    pub code: String,
    pub target: String,
}

pub trait AsDataManager: Send {
    /// Insert a new edge
    fn insert_edge_v(
        &mut self,
        edge_v: &Vec<Edge>,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// Clear all edge with `source` and `code` and insert a new edge
    fn clear(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// Clear all edge with `source` and `code` and insert a new edge
    fn rclear(
        &mut self,
        code: &str,
        target: &str,
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

    /// Get all targets from `source->code`
    fn get_source_v(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send;

    async fn commit(&mut self) -> io::Result<()>;
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
    async fn insert_edge_v(&mut self, edge_v: &Vec<Edge>) -> io::Result<()> {
        for edge in edge_v {
            if is_temp(&edge.source, &edge.code, &edge.target) {
                self.mem_table
                    .insert_temp_edge(&edge.source, &edge.code, &edge.target);
            } else {
                self.mem_table
                    .insert_edge(&edge.source, &edge.code, &edge.target);
            }
        }
        Ok(())
    }

    async fn clear(&mut self, source: &str, code: &str) -> io::Result<()> {
        self.mem_table.delete_edge_with_source_code(source, code);
        if !is_temp(source, code, "") {
            dao::delete_edge_with_source_code(&mut self.conn, source, code).await?;
        }
        Ok(())
    }

    async fn rclear(&mut self, code: &str, target: &str) -> io::Result<()> {
        self.mem_table.delete_edge_with_code_target(code, target);
        if !is_temp("", code, target) {
            dao::delete_edge_with_code_target(&mut self.conn, code, target).await?;
        }
        Ok(())
    }

    async fn get_target(&mut self, source: &str, code: &str) -> io::Result<String> {
        if let Some(target) = self.mem_table.get_target(source, code) {
            Ok(target)
        } else {
            let target = dao::get_target(&mut self.conn, source, code).await?;
            self.mem_table.append_exists_edge(source, code, &target);
            Ok(target)
        }
    }

    async fn get_source(&mut self, code: &str, target: &str) -> io::Result<String> {
        if let Some(source) = self.mem_table.get_source(code, target) {
            Ok(source)
        } else {
            let source = dao::get_source(&mut self.conn, code, target).await?;
            self.mem_table.append_exists_edge(&source, code, target);
            Ok(source)
        }
    }

    async fn get_target_v(&mut self, source: &str, code: &str) -> io::Result<Vec<String>> {
        let r = self.mem_table.get_target_v_unchecked(source, code);
        if r.is_empty() {
            let r = dao::get_target_v(&mut self.conn, source, code).await?;
            for target in &r {
                self.mem_table.append_exists_edge(source, code, target);
            }
            Ok(r)
        } else {
            Ok(r)
        }
    }

    async fn get_source_v(&mut self, code: &str, target: &str) -> io::Result<Vec<String>> {
        let r = self.mem_table.get_source_v_unchecked(code, target);
        if r.is_empty() {
            let r = dao::get_source_v(&mut self.conn, code, target).await?;
            for source in &r {
                self.mem_table.append_exists_edge(source, code, target);
            }
            Ok(r)
        } else {
            Ok(r)
        }
    }

    async fn commit(&mut self) -> io::Result<()> {
        commit(self).await
    }
}
