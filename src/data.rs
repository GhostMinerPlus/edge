use sqlx::MySqlConnection;

use crate::err::Result;

mod dao;

fn is_temp(source: &str, code: &str, target: &str) -> bool {
    source.starts_with('$') || code.starts_with('$') || target.starts_with('$')
}

async fn clear(dm: &mut DataManager<'_>, source: &str, code: &str) -> Result<()> {
    dm.mem_table.delete_edge_with_source_code(source, code);
    if !is_temp(source, code, "") {
        dao::delete_edge_with_source_code(&mut dm.conn, source, code).await?;
    }
    Ok(())
}

async fn rclear(dm: &mut DataManager<'_>, code: &str, target: &str) -> Result<()> {
    dm.mem_table.delete_edge_with_code_target(code, target);
    if !is_temp("", code, target) {
        dao::delete_edge_with_code_target(&mut dm.conn, code, target).await?;
    }
    Ok(())
}

// Public
pub mod mem_table;

pub trait AsDataManager: Send {
    fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Get a target from `source->code`
    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = Result<String>> + Send;

    /// Get a source from `target<-code`
    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = Result<String>> + Send;

    /// Get all targets from `source->code`
    fn get_target_v(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    /// Get all targets from `source->code`
    fn get_source_v(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    async fn commit(&mut self) -> Result<()>;
}

pub struct DataManager<'a> {
    conn: &'a mut MySqlConnection,
    mem_table: &'a mut mem_table::MemTable,
}

impl<'a> DataManager<'a> {
    pub fn new(conn: &'a mut MySqlConnection, mem_table: &'a mut mem_table::MemTable) -> Self {
        Self { conn, mem_table }
    }
}

impl<'a> AsDataManager for DataManager<'a> {
    async fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Result<()> {
        if !is_temp(source, code, "") && self.mem_table.get_target(source, code).is_none() {
            let r = dao::get_target_v(&mut self.conn, source, code).await?;
            for target in &r {
                self.mem_table.append_exists_edge(source, code, target);
            }
        }
        for target in target_v {
            if is_temp(source, code, target) {
                self.mem_table.insert_temp_edge(source, code, target);
            } else {
                self.mem_table.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Result<()> {
        if !is_temp("", code, target) && self.mem_table.get_source(code, target).is_none() {
            let r = dao::get_source_v(&mut self.conn, code, target).await?;
            for source in &r {
                self.mem_table.append_exists_edge(source, code, target);
            }
        }
        for source in source_v {
            if is_temp(source, code, target) {
                self.mem_table.insert_temp_edge(source, code, target);
            } else {
                self.mem_table.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Result<()> {
        clear(self, source, code).await?;
        for target in target_v {
            if is_temp(source, code, target) {
                self.mem_table.insert_temp_edge(source, code, target);
            } else {
                self.mem_table.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Result<()> {
        rclear(self, code, target).await?;
        for source in source_v {
            if is_temp(source, code, target) {
                self.mem_table.insert_temp_edge(source, code, target);
            } else {
                self.mem_table.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn get_target(&mut self, source: &str, code: &str) -> Result<String> {
        if let Some(target) = self.mem_table.get_target(source, code) {
            Ok(target)
        } else {
            let target = dao::get_target(&mut self.conn, source, code).await?;
            Ok(target)
        }
    }

    async fn get_source(&mut self, code: &str, target: &str) -> Result<String> {
        if let Some(source) = self.mem_table.get_source(code, target) {
            Ok(source)
        } else {
            let source = dao::get_source(&mut self.conn, code, target).await?;
            Ok(source)
        }
    }

    async fn get_target_v(&mut self, source: &str, code: &str) -> Result<Vec<String>> {
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

    async fn get_source_v(&mut self, code: &str, target: &str) -> Result<Vec<String>> {
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

    async fn commit(&mut self) -> Result<()> {
        dao::insert_edge_mp(self.conn, &self.mem_table.take()).await
    }
}
