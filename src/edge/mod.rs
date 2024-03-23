mod inc;

use serde::Deserialize;
use std::io;

use crate::{data::AsDataManager, mem_table::new_point};

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
    log::debug!("invoke_inc: {:?}", inc);
    match inc.code.as_str() {
        "clear" => {
            inc::clear(dm, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "return" => {
            let target_v = inc::get_all_by_path(dm, Path::from_str(&inc.target)).await?;
            Ok(InvokeResult::Return(target_v))
        }
        "dump" => {
            inc::dump(dm, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        "dc_ns" => {
            inc::delete_code_without_source(dm, &inc.source, &inc.target).await?;
            Ok(InvokeResult::Jump(1))
        }
        _ => {
            let source_v = inc::get_all_by_path(dm, Path::from_str(&inc.source)).await?;
            log::debug!("unwraped {} to {:?}", inc.source, source_v);
            let target_v = inc::get_all_by_path(dm, Path::from_str(&inc.target)).await?;
            log::debug!("unwraped {} to {:?}", inc.target, target_v);

            for source in &source_v {
                for target in &target_v {
                    log::debug!("inserting an edge: {source} {} {target}", inc.code);
                    dm.insert_edge(source, &inc.code, target).await?;
                    let listener_v = dm.get_target_v(&inc.code, "listener").await?;
                    for listener in &listener_v {
                        let inc_h_v = dm.get_target_v(&listener, "inc").await?;
                        let inc_v = dump_inc_v(dm, &inc_h_v).await?;

                        let mut new_root = format!("${}", new_point());
                        dm.insert_edge(&new_root, "$source", source).await?;
                        dm.insert_edge(&new_root, "$code", &inc.code).await?;
                        dm.insert_edge(&new_root, "$target", target).await?;
                        let mut pos = 0i32;
                        while (pos as usize) < inc_v.len() {
                            let inc = unwrap_inc(dm, &mut new_root, &inc_v[pos as usize]).await?;
                            match invoke_inc(dm, root, &inc).await? {
                                InvokeResult::Jump(step) => pos += step,
                                InvokeResult::Return(_) => break,
                            }
                        }
                    }
                }
            }

            Ok(InvokeResult::Jump(1))
        }
    }
}

async fn unwrap_inc(dm: &mut impl AsDataManager, root: &str, inc: &Inc) -> io::Result<Inc> {
    let path = inc::unwrap_value(root, &inc.code).await?;
    let code_v = inc::get_all_by_path(dm, Path::from_str(&path)).await?;
    let code = if code_v.is_empty() {
        String::default()
    } else {
        code_v[0].clone()
    };
    let inc = Inc {
        source: inc::unwrap_value(root, &inc.source).await?,
        code,
        target: inc::unwrap_value(root, &inc.target).await?,
    };
    Ok(inc)
}

// Public
pub use self::inc::Path;

#[derive(Clone, Deserialize, Debug)]
pub struct Inc {
    pub source: String,
    pub code: String,
    pub target: String,
}

pub enum InvokeResult {
    Jump(i32),
    Return(Vec<String>),
}

pub trait AsEdgeEngine {
    async fn invoke_inc_v(
        &mut self,
        root: &mut String,
        inc_v: &Vec<Inc>,
    ) -> io::Result<Vec<String>>;

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
    async fn invoke_inc_v(
        &mut self,
        root: &mut String,
        inc_v: &Vec<Inc>,
    ) -> io::Result<Vec<String>> {
        let mut pos = 0i32;
        let mut rs = Vec::new();
        log::debug!("inc_v.len(): {}", inc_v.len());
        while (pos as usize) < inc_v.len() {
            log::debug!("pos: {}", pos);
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

        fn clear(
            &mut self,
            source: &str,
            code: &str,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            self.mem_table.delete_edge_with_source_code(source, code);
            async { Ok(()) }
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

        async fn dump(
            &mut self,
            _path: &str,
            _item_v: &Vec<String>,
        ) -> std::io::Result<json::Array> {
            todo!()
        }

        async fn commit(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        async fn delete_code_without_source(
            &mut self,
            _code: &str,
            _source_code: &str,
        ) -> std::io::Result<()> {
            Ok(())
        }

        async fn delete_code_without_target(
            &mut self,
            _code: &str,
            _target_code: &str,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn get_source_v(
            &mut self,
            _code: &str,
            _target: &str,
        ) -> impl std::future::Future<Output = std::io::Result<Vec<String>>> + Send {
            async { todo!() }
        }

        async fn flush(&mut self) -> std::io::Result<()> {
            todo!()
        }
    }

    #[test]
    fn test() {
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
                Inc {
                    source: "$".to_string(),
                    code: "return".to_string(),
                    target: "edge->aim".to_string(),
                },
            ];

            let mut edge_engine = EdgeEngine::new(dm);
            let rs = edge_engine.invoke_inc_v(&mut root, &inc_v).await.unwrap();
            edge_engine.commit().await.unwrap();
            assert_eq!(rs.len(), 1);
            assert_eq!(rs[0], "target");
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(task);
    }
}
