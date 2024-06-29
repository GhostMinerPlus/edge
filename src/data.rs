use std::{future, io, pin::Pin, sync::Arc};

use edge_lib::{
    data::{AsDataManager, Auth},
    util::Path,
};
use sqlx::{MySql, Pool};

mod dao;

#[derive(Clone)]
pub struct DbDataManager {
    auth: Auth,
    pool: Pool<MySql>,
}

impl DbDataManager {
    pub fn new(global: Pool<MySql>, auth: Auth) -> Self {
        Self {
            auth,
            pool: global,
        }
    }
}

impl AsDataManager for DbDataManager {
    fn get_auth(&self) -> Auth {
        self.auth.clone()
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            auth,
            pool: self.pool.clone(),
        })
    }

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        Box::pin(future::ready(Ok(())))
    }

    fn append(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            let root_v = this.get(&path).await?;
            for source in &root_v {
                dao::insert_edge(this.pool.clone(), &this.auth, source, &step.code, &item_v)
                    .await?;
            }
            Ok(())
        })
    }

    fn set(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            let root_v = this.get(&path).await?;
            for source in &root_v {
                dao::delete_edge_with_source_code(
                    this.pool.clone(),
                    &this.auth,
                    source,
                    &step.code,
                )
                .await?;
            }
            for source in &root_v {
                dao::insert_edge(this.pool.clone(), &this.auth, source, &step.code, &item_v)
                    .await?;
            }
            Ok(())
        })
    }

    fn get(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        if path.step_v.is_empty() {
            if path.root.is_empty() {
                return Box::pin(future::ready(Ok(vec![])));
            }
            return Box::pin(future::ready(Ok(vec![path.root.clone()])));
        }
        let this = self.clone();
        let path = path.clone();
        Box::pin(async move { dao::get(this.pool.clone(), &this.auth, &path).await })
    }

    fn clear(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move { dao::clear(this.pool, &this.auth).await })
    }
}
