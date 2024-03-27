use crate::{
    data::AsDataManager,
    err::{Error, ErrorKind, Result},
};

mod function;
mod graph;
mod inc;
mod util;

#[async_recursion::async_recursion]
async fn invoke_inc(dm: &mut impl AsDataManager, root: &str, inc: &inc::Inc) -> Result<()> {
    log::debug!("invoke_inc: {:?}", inc);
    let input_item_v = util::get_all_by_path(dm, graph::Path::from_str(&inc.input)).await?;
    let input1_item_v = util::get_all_by_path(dm, graph::Path::from_str(&inc.input1)).await?;
    let rs = match inc.function.as_str() {
        //
        "new" => function::new(dm, input_item_v, input1_item_v).await?,
        "line" => function::line(dm, input_item_v, input1_item_v).await?,
        "rand" => function::rand(dm, input_item_v, input1_item_v).await?,
        //
        "append" => function::append(dm, input_item_v, input1_item_v).await?,
        "distinct" => function::distinct(dm, input_item_v, input1_item_v).await?,
        "left" => function::left(dm, input_item_v, input1_item_v).await?,
        "inner" => function::inner(dm, input_item_v, input1_item_v).await?,
        //
        "+" => function::add(dm, input_item_v, input1_item_v).await?,
        "-" => function::minus(dm, input_item_v, input1_item_v).await?,
        "*" => function::mul(dm, input_item_v, input1_item_v).await?,
        "/" => function::div(dm, input_item_v, input1_item_v).await?,
        "%" => function::rest(dm, input_item_v, input1_item_v).await?,
        //
        "==" => function::equal(dm, input_item_v, input1_item_v).await?,
        ">" => function::greater(dm, input_item_v, input1_item_v).await?,
        "<" => function::smaller(dm, input_item_v, input1_item_v).await?,
        //
        "sort" => function::sort(dm, input_item_v, input1_item_v).await?,
        //
        "count" => function::count(dm, input_item_v, input1_item_v).await?,
        "sum" => function::sum(dm, input_item_v, input1_item_v).await?,
        //
        "=" => function::set(dm, input_item_v, input1_item_v).await?,
        _ => {
            let inc_v = util::dump_inc_v(dm, &inc.function).await?;
            let new_root = format!("${}", uuid::Uuid::new_v4().to_string());
            util::asign(dm, &format!("{new_root}->$input"), "=", input_item_v).await?;
            util::asign(dm, &format!("{new_root}->$input1"), "=", input1_item_v).await?;
            log::debug!("inc_v.len(): {}", inc_v.len());
            for inc in &inc_v {
                let inc = parser::unwrap_inc(dm, &new_root, inc).await?;
                invoke_inc(dm, root, &inc).await?;
            }
            util::get_all_by_path(dm, graph::Path::from_str(&format!("{new_root}->$output")))
                .await?
        }
    };
    util::asign(dm, &inc.output, &inc.operator, rs).await
}

async fn invoke_inc_v(
    dm: &mut impl AsDataManager,
    root: &str,
    inc_v: &Vec<inc::Inc>,
) -> Result<Vec<String>> {
    log::debug!("inc_v.len(): {}", inc_v.len());
    for inc in inc_v {
        let inc = parser::unwrap_inc(dm, &root, inc).await?;
        invoke_inc(dm, root, &inc).await?;
    }
    util::get_all_by_path(dm, graph::Path::from_str(&format!("{root}->$output"))).await
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
) -> Result<()> {
    if script_tree.is_empty() {
        return Ok(());
    }
    if let json::JsonValue::Object(script_tree) = script_tree {
        for (script, v) in script_tree.iter() {
            let root = format!("${}", uuid::Uuid::new_v4().to_string());
            util::asign(
                dm,
                &format!("{root}->$input"),
                "+=",
                vec![input.to_string()],
            )
            .await?;
            let rs = invoke_inc_v(dm, &root, &parser::parse_script(script)?).await?;
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
        Err(Error::new(ErrorKind::Other, msg))
    }
}

// Public
pub mod parser;

pub trait AsEdgeEngine {
    async fn execute(&mut self, script_tree: &json::JsonValue) -> Result<json::JsonValue>;

    async fn require(
        &mut self,
        target: &Vec<inc::Inc>,
        constraint: &Vec<inc::Inc>,
    ) -> Result<Vec<Vec<inc::Inc>>>;

    async fn commit(&mut self) -> Result<()>;
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
    async fn execute(&mut self, script_tree: &json::JsonValue) -> Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        execute(&mut self.dm, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    async fn require(
        &mut self,
        _target: &Vec<inc::Inc>,
        _constraint: &Vec<inc::Inc>,
    ) -> Result<Vec<Vec<inc::Inc>>> {
        Ok(Vec::new())
    }

    async fn commit(&mut self) -> Result<()> {
        self.dm.commit().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{data::mem_table::MemTable, data::AsDataManager, err::Result};

    use super::{AsEdgeEngine, EdgeEngine};

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
        ) -> impl std::future::Future<Output = Result<String>> + Send {
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
        ) -> impl std::future::Future<Output = Result<String>> + Send {
            async {
                if let Some(source) = self.mem_table.get_source(code, target) {
                    Ok(source)
                } else {
                    Ok(String::new())
                }
            }
        }

        async fn get_target_v(&mut self, source: &str, code: &str) -> Result<Vec<String>> {
            Ok(self.mem_table.get_target_v_unchecked(source, code))
        }

        async fn commit(&mut self) -> Result<()> {
            Ok(())
        }

        fn get_source_v(
            &mut self,
            _code: &str,
            _target: &str,
        ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send {
            async { todo!() }
        }

        async fn append_target_v(
            &mut self,
            source: &str,
            code: &str,
            target_v: &Vec<String>,
        ) -> Result<()> {
            for target in target_v {
                self.mem_table.insert_temp_edge(source, code, target);
            }
            Ok(())
        }

        async fn append_source_v(
            &mut self,
            source_v: &Vec<String>,
            code: &str,
            target: &str,
        ) -> Result<()> {
            for source in source_v {
                self.mem_table.insert_temp_edge(source, code, target);
            }
            Ok(())
        }

        async fn set_target_v(
            &mut self,
            source: &str,
            code: &str,
            target_v: &Vec<String>,
        ) -> Result<()> {
            self.mem_table.delete_edge_with_source_code(source, code);
            for target in target_v {
                self.mem_table.insert_temp_edge(source, code, target);
            }
            Ok(())
        }

        async fn set_source_v(
            &mut self,
            source_v: &Vec<String>,
            code: &str,
            target: &str,
        ) -> Result<()> {
            self.mem_table.delete_edge_with_code_target(code, target);
            for source in source_v {
                self.mem_table.insert_temp_edge(source, code, target);
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
