use std::{collections::HashSet, io, mem, pin::Pin, sync::Arc};

use edge_lib::{data::AsDataManager, mem_table::MemTable};
use sqlx::{MySql, Pool};
use tokio::sync::Mutex;

mod dao;

fn is_temp(code: &str) -> bool {
    code.starts_with('$')
}

struct CacheTable {
    cache: MemTable,
    delete_list_by_source: HashSet<(String, String)>,
    delete_list_by_target: HashSet<(String, String)>,
}

impl CacheTable {
    fn new() -> Self {
        Self {
            cache: MemTable::new(),
            delete_list_by_source: Default::default(),
            delete_list_by_target: Default::default(),
        }
    }
}

// Public
#[derive(Clone)]
pub struct DataManager {
    pool: Pool<MySql>,
    cache_table: Arc<Mutex<CacheTable>>,
}

impl DataManager {
    pub fn new(pool: Pool<MySql>) -> Self {
        Self {
            pool,
            cache_table: Arc::new(Mutex::new(CacheTable::new())),
        }
    }
}

impl AsDataManager for DataManager {
    fn divide(&self) -> Box<dyn AsDataManager> {
        Box::new(Self {
            pool: self.pool.clone(),
            cache_table: Arc::new(Mutex::new(CacheTable::new())),
        })
    }

    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<String>> + Send>> {
        let dm = self.clone();
        let (source, code) = (source.to_string(), code.to_string());
        Box::pin(async move {
            let cache_table = dm.cache_table.lock().await;
            if let Some(target) = cache_table.cache.get_target(&source, &code) {
                return Ok(target);
            }
            Ok(dao::get_target(dm.pool, &source, &code).await?)
        })
    }

    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<String>> + Send>> {
        let dm = self.clone();
        let (code, target) = (code.to_string(), target.to_string());
        Box::pin(async move {
            let cache_table = dm.cache_table.lock().await;
            if let Some(source) = cache_table.cache.get_source(&code, &target) {
                return Ok(source);
            }
            Ok(dao::get_source(dm.pool, &code, &target).await?)
        })
    }

    fn get_target_v(
        &mut self,
        source: &str,
        code: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        let dm = self.clone();
        let (source, code) = (source.to_string(), code.to_string());
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            let rs = cache_table.cache.get_target_v_unchecked(&source, &code);
            if !rs.is_empty() {
                return Ok(rs);
            }

            let rs = dao::get_target_v(dm.pool, &source, &code).await?;
            for target in &rs {
                cache_table.cache.insert_temp_edge(&source, &code, target);
            }
            Ok(rs)
        })
    }

    fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let dm = self.clone();
        let (source, code, target_v) = (source.to_string(), code.to_string(), target_v.clone());
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            if is_temp(&code) {
                for target in &target_v {
                    cache_table.cache.insert_temp_edge(&source, &code, target);
                }
                return Ok(());
            }

            if cache_table.cache.get_target(&source, &code).is_none() {
                let r = dao::get_target_v(dm.pool, &source, &code).await?;
                for target in &r {
                    cache_table.cache.insert_temp_edge(&source, &code, target);
                }
            }
            for target in &target_v {
                cache_table.cache.insert_edge(&source, &code, target);
            }
            Ok(())
        })
    }

    fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let dm = self.clone();
        let (source_v, code, target) = (source_v.clone(), code.to_string(), target.to_string());
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            if is_temp(&code) {
                for source in &source_v {
                    cache_table.cache.insert_temp_edge(source, &code, &target);
                }
                return Ok(());
            }

            if cache_table.cache.get_source(&code, &target).is_none() {
                let rs = dao::get_source_v(dm.pool, &code, &target).await?;
                for source in &rs {
                    cache_table.cache.insert_temp_edge(source, &code, &target);
                }
            }
            for source in &source_v {
                cache_table.cache.insert_edge(source, &code, &target);
            }
            Ok(())
        })
    }

    fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let dm = self.clone();
        let (source, code, target_v) = (source.to_string(), code.to_string(), target_v.clone());
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            cache_table
                .cache
                .delete_edge_with_source_code(&source, &code);
            if is_temp(&code) {
                for target in &target_v {
                    cache_table.cache.insert_temp_edge(&source, &code, target);
                }
            } else {
                cache_table
                    .delete_list_by_source
                    .insert((source.to_string(), code.to_string()));
                for target in &target_v {
                    cache_table.cache.insert_edge(&source, &code, target);
                }
            }
            Ok(())
        })
    }

    fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let dm = self.clone();
        let (source_v, code, target) = (source_v.clone(), code.to_string(), target.to_string());
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            cache_table
                .cache
                .delete_edge_with_code_target(&code, &target);
            if is_temp(&code) {
                for source in &source_v {
                    cache_table.cache.insert_temp_edge(source, &code, &target);
                }
            } else {
                cache_table
                    .delete_list_by_target
                    .insert((code.to_string(), target.to_string()));
                for source in &source_v {
                    cache_table.cache.insert_edge(source, &code, &target);
                }
            }
            Ok(())
        })
    }

    fn get_source_v(
        &mut self,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        let dm = self.clone();
        let (code, target) = (code.to_string(), target.to_string());
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            let r = cache_table.cache.get_source_v_unchecked(&code, &target);
            if r.is_empty() {
                let r = dao::get_source_v(dm.pool, &code, &target).await?;
                for source in &r {
                    cache_table.cache.insert_temp_edge(source, &code, &target);
                }
                Ok(r)
            } else {
                Ok(r)
            }
        })
    }

    fn commit(&mut self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let dm = self.clone();
        Box::pin(async move {
            let mut cache_table = dm.cache_table.lock().await;
            for (source, code) in mem::take(&mut cache_table.delete_list_by_source) {
                dao::delete_edge_with_source_code(dm.pool.clone(), &source, &code).await?;
            }
            for (code, target) in mem::take(&mut cache_table.delete_list_by_target) {
                dao::delete_edge_with_code_target(dm.pool.clone(), &code, &target).await?;
            }
            dao::insert_edge_mp(dm.pool.clone(), &cache_table.cache.take()).await?;
            Ok(())
        })
    }
}
