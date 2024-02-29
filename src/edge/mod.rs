mod inc;

use serde::Deserialize;
use std::io;

use crate::{data::AsDataManager, mem_table::new_point};

mod graph;

async fn dump_inc_v(dm: &mut impl AsDataManager, inc_h_v: &Vec<String>) -> io::Result<Vec<Inc>> {
    let mut inc_v = Vec::with_capacity(inc_h_v.len());
    for inc_h in inc_h_v {
        inc_v.push(Inc {
            source: dm.get_target(inc_h, "source").await?,
            code: dm.get_target(inc_h, "code").await?,
            target: dm.get_target(inc_h, "target").await?,
        });
    }
    Ok(inc_v)
}

#[async_recursion::async_recursion]
async fn invoke_inc(
    dm: &mut impl AsDataManager,
    root: &mut String,
    inc: &Inc,
) -> io::Result<InvokeResult> {
    match inc.code.as_str() {
        "return" => Ok(InvokeResult::Return(inc.target.clone())),
        "dump" => Ok(InvokeResult::Return(inc::dump(dm, &inc.target).await?)),
        "asign" => {
            inc::asign(dm, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "delete" => {
            inc::delete(dm, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "dc" => {
            inc::delete_code(dm, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "dc_ns" => {
            let code = graph::get_target_anyway(dm, &inc.target, "$code").await?;
            let source_code = graph::get_target_anyway(dm, &inc.target, "$source_code").await?;
            inc::delete_code_without_source(dm, &code, &source_code).await?;
            Ok(InvokeResult::Jump(1))
        }
        "dc_nt" => {
            let code = graph::get_target_anyway(dm, &inc.target, "$code").await?;
            let target_code = graph::get_target_anyway(dm, &inc.target, "$target_code").await?;
            inc::delete_code_without_target(dm, &code, &target_code).await?;
            Ok(InvokeResult::Jump(1))
        }
        "set" => {
            inc::set(dm, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "append" => {
            inc::append(dm, &root, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        _ => {
            dm.insert_edge(&inc.source, &inc.code, &inc.target)
                .await?;
            let listener_v = dm.get_target_v(&inc.code, "listener").await?;
            for listener in &listener_v {
                let inc_h_v = dm.get_target_v(&listener, "inc").await?;
                let inc_v = dump_inc_v(dm, &inc_h_v).await?;

                let mut new_root = format!("${}", new_point());
                dm.insert_edge(&new_root, "$source", &inc.source).await?;
                dm.insert_edge(&new_root, "$code", &inc.code).await?;
                dm.insert_edge(&new_root, "$target", &inc.target).await?;
                let mut pos = 0i32;
                while (pos as usize) < inc_v.len() {
                    let inc = unwrap_inc(dm, &mut new_root, &inc_v[pos as usize]).await?;
                    match invoke_inc(dm, root, &inc).await? {
                        InvokeResult::Jump(step) => pos += step,
                        InvokeResult::Return(_) => break,
                    }
                }
            }
            Ok(InvokeResult::Jump(1))
        }
    }
}

async fn unwrap_inc(dm: &mut impl AsDataManager, root: &str, inc: &Inc) -> io::Result<Inc> {
    let inc = Inc {
        source: inc::unwrap_value(dm, root, &inc.source).await?,
        code: inc::unwrap_value(dm, root, &inc.code).await?,
        target: inc::unwrap_value(dm, root, &inc.target).await?,
    };
    log::debug!("{:?}", inc);
    Ok(inc)
}

// Public
#[derive(Clone, Deserialize, Debug)]
pub struct Inc {
    pub source: String,
    pub code: String,
    pub target: String,
}

pub enum InvokeResult {
    Jump(i32),
    Return(String),
}

pub trait AsEdgeEngine {
    async fn invoke_inc_v(&mut self, root: &mut String, inc_v: &Vec<Inc>) -> io::Result<String>;

    async fn commit(&mut self) -> io::Result<()>;
}

pub struct EdgeEngine<DM: AsDataManager> {
    dm: DM,
}

impl<DM: AsDataManager> EdgeEngine<DM> {
    pub fn new(dm: DM) -> Self {
        Self { dm }
    }
}

impl<DM: AsDataManager> AsEdgeEngine for EdgeEngine<DM> {
    async fn invoke_inc_v(&mut self, root: &mut String, inc_v: &Vec<Inc>) -> io::Result<String> {
        let mut pos = 0i32;
        let mut rs = String::new();
        while (pos as usize) < inc_v.len() {
            log::debug!("pos: {},inc_v.len(): {}", pos, inc_v.len());
            let inc = unwrap_inc(&mut self.dm, &root, &inc_v[pos as usize]).await?;
            match invoke_inc(&mut self.dm, root, &inc).await? {
                InvokeResult::Jump(step) => pos += step,
                InvokeResult::Return(s) => {
                    rs = s;
                    break;
                }
            }
        }
        Ok(rs)
    }

    async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        data::AsDataManager,
        mem_table::{new_point, MemTable},
    };

    use super::{AsEdgeEngine, EdgeEngine, Inc};

    struct FakeDataManager {
        mem_table: MemTable,
    }

    impl FakeDataManager {
        pub fn new() -> Self {
            Self {
                mem_table: MemTable::new(),
            }
        }
    }

    impl AsDataManager for FakeDataManager {
        fn insert_edge(
            &mut self,
            source: &str,
            code: &str,
            target: &str,
        ) -> impl std::future::Future<Output = std::io::Result<String>> + Send {
            let id = self.mem_table.insert_edge(source, code, target);
            async { Ok(id) }
        }

        fn set_target(
            &mut self,
            source: &str,
            code: &str,
            target: &str,
        ) -> impl std::future::Future<Output = std::io::Result<String>> + Send {
            let id = self
                .mem_table
                .set_target(source, code, target)
                .unwrap_or_default();
            async { Ok(id) }
        }

        fn get_target(
            &mut self,
            source: &str,
            code: &str,
        ) -> impl std::future::Future<Output = std::io::Result<String>> + Send {
            async {
                if let Some(target) = self.mem_table.get_target(source, code) {
                    Ok(target)
                } else {
                    Ok(String::new())
                }
            }
        }

        fn get_source(
            &mut self,
            code: &str,
            target: &str,
        ) -> impl std::future::Future<Output = std::io::Result<String>> + Send {
            async {
                if let Some(source) = self.mem_table.get_source(code, target) {
                    Ok(source)
                } else {
                    Ok(String::new())
                }
            }
        }

        async fn get_target_v(&mut self, source: &str, code: &str) -> std::io::Result<Vec<String>> {
            Ok(self.mem_table.get_target_v_unchecked(source, code))
        }

        async fn get_list(
            &mut self,
            root: &str,
            dimension_v: &Vec<String>,
            attr_v: &Vec<String>,
        ) -> std::io::Result<json::Array> {
            todo!()
        }

        async fn commit(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        async fn delete(&mut self, point: &str) -> std::io::Result<()> {
            Ok(())
        }

        async fn delete_code(&mut self, code: &str) -> std::io::Result<()> {
            Ok(())
        }

        async fn delete_code_without_source(
            &mut self,
            code: &str,
            source_code: &str,
        ) -> std::io::Result<()> {
            Ok(())
        }

        async fn delete_code_without_target(
            &mut self,
            code: &str,
            target_code: &str,
        ) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("DEBUG"))
        .init();

        let task = async {
            let dm = FakeDataManager::new();
            let mut root = new_point();
            let inc_v = vec![
                Inc {
                    source: "aim".to_string(),
                    code: "listener".to_string(),
                    target: "test_listener".to_string(),
                },
                Inc {
                    source: "test_listener".to_string(),
                    code: "inc".to_string(),
                    target: "test_listener_1".to_string(),
                },
                Inc {
                    source: "edge".to_string(),
                    code: "aim".to_string(),
                    target: "target".to_string(),
                },
            ];

            let mut edge_engine = EdgeEngine::new(dm);
            edge_engine.invoke_inc_v(&mut root, &inc_v).await.unwrap();
            edge_engine.commit().await.unwrap();
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(task);
    }
}
