mod inc;

use serde::Deserialize;
use std::io;

use crate::data::AsDataManager;

#[async_recursion::async_recursion]
async fn get_all_by_path(dm: &mut impl AsDataManager, mut path: Path) -> io::Result<Vec<String>> {
    if path.step_v.is_empty() {
        if path.root.is_empty() {
            return Ok(Vec::new());
        } else {
            return Ok(vec![path.root.clone()]);
        }
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

async fn asign(
    dm: &mut impl AsDataManager,
    output: &str,
    operator: &str,
    item_v: Vec<String>,
) -> io::Result<()> {
    let mut output_path = Path::from_str(output);
    let last_step = output_path.step_v.pop().unwrap();
    let root_v = get_all_by_path(dm, output_path).await?;
    if last_step.arrow == "->" {
        for source in &root_v {
            if operator == "=" {
                dm.set_target_v(source, &last_step.code, &item_v).await?;
            } else {
                dm.append_target_v(source, &last_step.code, &item_v).await?;
            }
        }
    } else {
        for target in &root_v {
            if operator == "=" {
                dm.set_source_v(&item_v, &last_step.code, target).await?;
            } else {
                dm.append_source_v(&item_v, &last_step.code, target).await?;
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
            operator: dm.get_target(inc_h, "operator").await?,
            function: dm.get_target(inc_h, "function").await?,
            input: dm.get_target(inc_h, "input").await?,
            input1: dm.get_target(inc_h, "input1").await?,
        });
    }
    Ok(inc_v)
}

#[async_recursion::async_recursion]
async fn invoke_inc(dm: &mut impl AsDataManager, root: &str, inc: &Inc) -> io::Result<()> {
    log::debug!("invoke_inc: {:?}", inc);
    let input_item_v = get_all_by_path(dm, Path::from_str(&inc.input)).await?;
    let input1_item_v = get_all_by_path(dm, Path::from_str(&inc.input1)).await?;
    let rs = match inc.function.as_str() {
        //
        "new" => inc::new(dm, input_item_v, input1_item_v).await?,
        "line" => inc::line(dm, input_item_v, input1_item_v).await?,
        "rand" => inc::rand(dm, input_item_v, input1_item_v).await?,
        //
        "append" => inc::append(dm, input_item_v, input1_item_v).await?,
        "distinct" => inc::distinct(dm, input_item_v, input1_item_v).await?,
        "left" => inc::left(dm, input_item_v, input1_item_v).await?,
        "inner" => inc::inner(dm, input_item_v, input1_item_v).await?,
        //
        "+" => inc::add(dm, input_item_v, input1_item_v).await?,
        "-" => inc::minus(dm, input_item_v, input1_item_v).await?,
        "*" => inc::mul(dm, input_item_v, input1_item_v).await?,
        "/" => inc::div(dm, input_item_v, input1_item_v).await?,
        "%" => inc::rest(dm, input_item_v, input1_item_v).await?,
        //
        "==" => inc::equal(dm, input_item_v, input1_item_v).await?,
        ">" => inc::greater(dm, input_item_v, input1_item_v).await?,
        "<" => inc::smaller(dm, input_item_v, input1_item_v).await?,
        //
        "sort" => inc::sort(dm, input_item_v, input1_item_v).await?,
        //
        "count" => inc::count(dm, input_item_v, input1_item_v).await?,
        "sum" => inc::sum(dm, input_item_v, input1_item_v).await?,
        //
        "=" => inc::set(dm, input_item_v, input1_item_v).await?,
        _ => {
            let inc_v = dump_inc_v(dm, &inc.function).await?;
            let new_root = format!("${}", uuid::Uuid::new_v4().to_string());
            asign(dm, &format!("{new_root}->$input"), "=", input_item_v).await?;
            asign(dm, &format!("{new_root}->$input1"), "=", input1_item_v).await?;
            log::debug!("inc_v.len(): {}", inc_v.len());
            for inc in &inc_v {
                let inc = unwrap_inc(dm, &new_root, inc).await?;
                invoke_inc(dm, root, &inc).await?;
            }
            get_all_by_path(dm, Path::from_str(&format!("{new_root}->$output"))).await?
        }
    };
    asign(dm, &inc.output, &inc.operator, rs).await
}

async fn get_one(dm: &mut impl AsDataManager, root: &str, id: &str) -> io::Result<String> {
    let path = unwrap_value(root, id).await?;
    let id_v = get_all_by_path(dm, Path::from_str(&path)).await?;
    if id_v.len() != 1 {
        return Err(io::Error::new(io::ErrorKind::NotFound, "need 1 but not"));
    }
    Ok(id_v[0].clone())
}

async fn unwrap_inc(dm: &mut impl AsDataManager, root: &str, inc: &Inc) -> io::Result<Inc> {
    let inc = Inc {
        output: unwrap_value(root, &inc.output).await?,
        operator: get_one(dm, root, &inc.operator).await?,
        function: get_one(dm, root, &inc.function).await?,
        input: unwrap_value(root, &inc.input).await?,
        input1: unwrap_value(root, &inc.input1).await?,
    };
    Ok(inc)
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

async fn invoke_inc_v(
    dm: &mut impl AsDataManager,
    root: &str,
    inc_v: &Vec<Inc>,
) -> io::Result<Vec<String>> {
    log::debug!("inc_v.len(): {}", inc_v.len());
    for inc in inc_v {
        let inc = unwrap_inc(dm, &root, inc).await?;
        invoke_inc(dm, root, &inc).await?;
    }
    get_all_by_path(dm, Path::from_str(&format!("{root}->$output"))).await
}

fn merge(p_tree: &mut json::JsonValue, s_tree: &mut json::JsonValue) {
    for (k, v) in s_tree.entries_mut() {
        if v.is_array() {
            if !p_tree.has_key(k) {
                let _ = p_tree.insert(k, json::array![]);
            }
            let _ = p_tree[k].push(v.clone());
        } else {
            if !p_tree.has_key(k) {
                let _ = p_tree.insert(k, json::object! {});
            }
            merge(&mut p_tree[k], v);
        }
    }
}

#[async_recursion::async_recursion]
async fn execute(
    dm: &mut impl AsDataManager,
    input: &str,
    script_tree: &json::JsonValue,
    out_tree: &mut json::JsonValue,
) -> io::Result<()> {
    if script_tree.is_empty() {
        return Ok(());
    }
    if let json::JsonValue::Object(script_tree) = script_tree {
        for (script, v) in script_tree.iter() {
            let root = format!("${}", uuid::Uuid::new_v4().to_string());
            asign(
                dm,
                &format!("{root}->$input"),
                "+=",
                vec![input.to_string()],
            )
            .await?;
            let rs = invoke_inc_v(dm, &root, &parse_script(script)?).await?;
            if v.is_empty() {
                let rs: json::JsonValue = rs.into();
                let _ = out_tree.insert(script, rs);
            } else {
                // fork
                let mut cur = json::object! {};
                for input in &rs {
                    let mut sub_out_tree = json::object! {};
                    execute(dm, input, v, &mut sub_out_tree).await?;
                    merge(&mut cur, &mut sub_out_tree);
                }
                let _ = out_tree.insert(script, cur);
            }
        }
        Ok(())
    } else {
        let msg = format!("can not parse {}", script_tree);
        log::error!("{msg}");
        Err(io::Error::new(io::ErrorKind::InvalidData, msg))
    }
}

fn parse_script(script: &str) -> io::Result<Vec<Inc>> {
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }
        // <output> <operator> <function> <input>
        let word_v: Vec<&str> = line.split(" ").collect();
        if word_v.len() != 5 {
            log::error!("while parsing script: word_v.len() != 5");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "while parsing script",
            ));
        }
        inc_v.push(Inc {
            output: word_v[0].trim().to_string(),
            operator: word_v[1].trim().to_string(),
            function: word_v[2].trim().to_string(),
            input: word_v[3].trim().to_string(),
            input1: word_v[4].trim().to_string(),
        });
    }
    Ok(inc_v)
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

impl Path {
    pub fn from_str(path: &str) -> Self {
        if path.is_empty() {
            return Path {
                root: String::new(),
                step_v: Vec::new(),
            };
        }
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
    pub operator: String,
    pub function: String,
    pub input: String,
    pub input1: String,
}

pub trait AsEdgeEngine {
    async fn execute(&mut self, script_tree: &json::JsonValue) -> io::Result<json::JsonValue>;

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
    async fn execute(&mut self, script_tree: &json::JsonValue) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        execute(&mut self.dm, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{data::AsDataManager, mem_table::MemTable};

    use super::{AsEdgeEngine, EdgeEngine};

    fn is_temp(source: &str, code: &str, target: &str) -> bool {
        source.starts_with('$') || code.starts_with('$') || target.starts_with('$')
    }

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

        async fn append_target_v(
            &mut self,
            source: &str,
            code: &str,
            target_v: &Vec<String>,
        ) -> io::Result<()> {
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
        ) -> io::Result<()> {
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
        ) -> io::Result<()> {
            self.mem_table.delete_edge_with_source_code(source, code);
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
        ) -> io::Result<()> {
            self.mem_table.delete_edge_with_code_target(code, target);
            for source in source_v {
                if is_temp(source, code, target) {
                    self.mem_table.insert_temp_edge(source, code, target);
                } else {
                    self.mem_table.insert_edge(source, code, target);
                }
            }
            Ok(())
        }
    }

    #[test]
    fn test() {
        let task = async {
            let dm = FakeDataManager::new();
            let root = format!(
                "$->$left = new 100 100
$->$right = new 100 100
$->$output = + $->$left $->$right"
            );
            let then = format!("$->$output = rand $->$input _");
            let then_tree = json::object! {};
            let mut root_tree = json::object! {};
            let _ = root_tree.insert(&then, then_tree);
            let mut script_tree = json::object! {};
            let _ = script_tree.insert(&root, root_tree);

            let mut edge_engine = EdgeEngine::new(dm);
            let rs = edge_engine.execute(&script_tree).await.unwrap();
            edge_engine.commit().await.unwrap();
            let rs = &rs[&root][&then];
            assert_eq!(rs.len(), 100);
            assert_eq!(rs[0].len(), 200);
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(task);
    }
}
