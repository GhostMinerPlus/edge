mod inc;

use serde::Deserialize;
use std::io;

use crate::data::{AsDataManager, Edge};

#[async_recursion::async_recursion]
async fn get_all_by_path(dm: &mut impl AsDataManager, mut path: Path) -> io::Result<Vec<String>> {
    if path.step_v.is_empty() {
        return Ok(vec![path.root.clone()]);
    }
    let root = path.root.clone();
    let step = path.step_v.remove(0);
    let curr_v = if step.arrow == "->" {
        dm.get_target_v(&root, &step.code).await?
    } else {
        dm.get_source_v(&step.code, &root).await?
    };
    let mut rs = Vec::new();
    for root in curr_v {
        rs.append(
            &mut get_all_by_path(
                dm,
                Path {
                    root,
                    step_v: path.step_v.clone(),
                },
            )
            .await?,
        );
    }
    Ok(rs)
}

async fn unwrap_value(root: &str, value: &str) -> io::Result<String> {
    if value == "?" {
        Ok(uuid::Uuid::new_v4().to_string())
    } else if value == "$" {
        Ok(root.to_string())
    } else if value == "_" {
        Ok("".to_string())
    } else if value.starts_with("$<-") {
        Ok(format!("{root}{}", &value[1..]))
    } else if value.starts_with("$->") {
        Ok(format!("{root}{}", &value[1..]))
    } else {
        Ok(value.to_string())
    }
}

async fn asign(dm: &mut impl AsDataManager, output: &str, item_v: Vec<String>) -> io::Result<()> {
    let mut output_path = Path::from_str(output);
    let last_step = output_path.step_v.pop().unwrap();
    let root_v = get_all_by_path(dm, output_path).await?;
    if last_step.arrow == "->" {
        for source in &root_v {
            dm.clear(source, &last_step.code).await?;
            for target in &item_v {
                dm.insert_edge_v(&vec![Edge {
                    source: source.clone(),
                    code: last_step.code.clone(),
                    target: target.clone(),
                }])
                .await?;
            }
        }
    } else {
        for target in &root_v {
            dm.rclear(&last_step.code, target).await?;
            for source in &item_v {
                dm.insert_edge_v(&vec![Edge {
                    source: source.clone(),
                    code: last_step.code.clone(),
                    target: target.clone(),
                }])
                .await?;
            }
        }
    }
    Ok(())
}

async fn dump_inc_v(dm: &mut impl AsDataManager, function: &str) -> io::Result<Vec<Inc>> {
    let inc_h_v = dm.get_target_v(function, "inc").await?;
    let mut inc_v = Vec::with_capacity(inc_h_v.len());
    for inc_h in &inc_h_v {
        inc_v.push(Inc {
            output: dm.get_target(inc_h, "output").await?,
            function: dm.get_target(inc_h, "function").await?,
            input: dm.get_target(inc_h, "input").await?,
            input1: dm.get_target(inc_h, "input1").await?,
        });
    }
    Ok(inc_v)
}

#[async_recursion::async_recursion]
async fn invoke_inc(dm: &mut impl AsDataManager, root: &mut String, inc: &Inc) -> io::Result<()> {
    log::debug!("invoke_inc: {:?}", inc);
    let input_item_v = get_all_by_path(dm, Path::from_str(&inc.input)).await?;
    let input1_item_v = get_all_by_path(dm, Path::from_str(&inc.input1)).await?;
    let rs = match inc.function.as_str() {
        "=" => inc::set(dm, input_item_v, input1_item_v).await?,
        "+" => inc::add(dm, input_item_v, input1_item_v).await?,
        "-" => inc::minus(dm, input_item_v, input1_item_v).await?,
        "append" => inc::append(dm, input_item_v, input1_item_v).await?,
        "new" => inc::new(dm, input_item_v, input1_item_v).await?,
        "sort" => inc::sort(dm, input_item_v, input1_item_v).await?,
        _ => {
            let inc_v = dump_inc_v(dm, &inc.function).await?;
            let new_root = format!("${}", uuid::Uuid::new_v4().to_string());
            asign(dm, &format!("{new_root}->$input"), input_item_v).await?;
            asign(dm, &format!("{new_root}->$input1"), input1_item_v).await?;
            log::debug!("inc_v.len(): {}", inc_v.len());
            for inc in &inc_v {
                let inc = unwrap_inc(dm, &new_root, inc).await?;
                invoke_inc(dm, root, &inc).await?;
            }
            get_all_by_path(dm, Path::from_str(&format!("{new_root}->$output"))).await?
        }
    };
    asign(dm, &inc.output, rs).await
}

async fn unwrap_inc(dm: &mut impl AsDataManager, root: &str, inc: &Inc) -> io::Result<Inc> {
    let path = unwrap_value(root, &inc.function).await?;
    let function_v = get_all_by_path(dm, Path::from_str(&path)).await?;
    if function_v.len() != 1 {
        return Err(io::Error::new(io::ErrorKind::NotFound, "unknown function"));
    }
    let inc = Inc {
        output: unwrap_value(root, &inc.output).await?,
        function: function_v[0].clone(),
        input: unwrap_value(root, &inc.input).await?,
        input1: unwrap_value(root, &inc.input1).await?,
    };
    Ok(inc)
}

// Public
#[derive(Clone)]
pub struct Step {
    pub arrow: String,
    pub code: String,
}

pub struct Path {
    pub root: String,
    pub step_v: Vec<Step>,
}

fn find_arrrow(path: &str) -> usize {
    let p = path.find("->");
    let q = path.find("<-");
    if p.is_none() && q.is_none() {
        path.len()
    } else {
        if p.is_some() && q.is_some() {
            let p = p.unwrap();
            let q = q.unwrap();
            std::cmp::min(p, q)
        } else if p.is_some() {
            p.unwrap()
        } else {
            q.unwrap()
        }
    }
}

impl Path {
    pub fn from_str(path: &str) -> Self {
        log::debug!("Path::from_str: {path}");
        if path.starts_with('"') {
            return Self {
                root: path[1..path.len() - 1].to_string(),
                step_v: Vec::new(),
            };
        }
        let mut s = find_arrrow(path);

        let root = path[0..s].to_string();
        if s == path.len() {
            return Self {
                root,
                step_v: Vec::new(),
            };
        }
        let mut tail = &path[s..];
        let mut step_v = Vec::new();
        loop {
            s = find_arrrow(&tail[2..]) + 2;
            step_v.push(Step {
                arrow: tail[0..2].to_string(),
                code: tail[2..s].to_string(),
            });
            if s == tail.len() {
                break;
            }
            tail = &tail[s..];
        }
        Self { root, step_v }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct Inc {
    pub output: String,
    pub function: String,
    pub input: String,
    pub input1: String,
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
        log::debug!("inc_v.len(): {}", inc_v.len());
        for inc in inc_v {
            let inc = unwrap_inc(&mut self.dm, &root, inc).await?;
            invoke_inc(&mut self.dm, root, &inc).await?;
        }
        get_all_by_path(&mut self.dm, Path::from_str(&format!("{root}->$output"))).await
    }

    async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        data::{AsDataManager, Edge},
        mem_table::MemTable,
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
        async fn insert_edge_v(&mut self, edge_v: &Vec<Edge>) -> std::io::Result<()> {
            for edge in edge_v {
                self.mem_table
                    .insert_edge(&edge.source, &edge.code, &edge.target);
            }
            Ok(())
        }

        fn clear(
            &mut self,
            source: &str,
            code: &str,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            self.mem_table.delete_edge_with_source_code(source, code);
            async { Ok(()) }
        }

        fn rclear(
            &mut self,
            code: &str,
            target: &str,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            self.mem_table.delete_edge_with_code_target(code, target);
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

        async fn commit(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        fn get_source_v(
            &mut self,
            _code: &str,
            _target: &str,
        ) -> impl std::future::Future<Output = std::io::Result<Vec<String>>> + Send {
            async { todo!() }
        }
    }

    #[test]
    fn test() {
        let task = async {
            let dm = FakeDataManager::new();
            let mut root = uuid::Uuid::new_v4().to_string();
            let inc_v = vec![
                Inc {
                    output: "$->$left".to_string(),
                    function: "new".to_string(),
                    input: "100".to_string(),
                    input1: "100".to_string(),
                },
                Inc {
                    output: "$->$right".to_string(),
                    function: "new".to_string(),
                    input: "100".to_string(),
                    input1: "100".to_string(),
                },
                Inc {
                    output: "$->$output".to_string(),
                    function: "+".to_string(),
                    input: "$->$left".to_string(),
                    input1: "$->$right".to_string(),
                },
            ];

            let mut edge_engine = EdgeEngine::new(dm);
            let rs = edge_engine.invoke_inc_v(&mut root, &inc_v).await.unwrap();
            edge_engine.commit().await.unwrap();
            assert_eq!(rs.len(), 100);
            assert_eq!(rs[0], "200");
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(task);
    }
}
