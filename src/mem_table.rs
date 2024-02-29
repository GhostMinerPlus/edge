use std::{collections::HashMap, mem::take};

fn insert(mp: &mut HashMap<(String, String), Vec<String>>, k: (String, String), v: String) {
    if let Some(id_v) = mp.get_mut(&k) {
        id_v.push(v);
    } else {
        mp.insert(k, vec![v]);
    }
}

// Public
pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub struct Edge {
    pub id: String,
    pub source: String,
    pub code: String,
    pub target: String,
    status: u8,
}

pub struct MemTable {
    edge_mp: HashMap<String, Edge>,
    inx_source_code: HashMap<(String, String), Vec<String>>,
    inx_code_target: HashMap<(String, String), Vec<String>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            edge_mp: HashMap::new(),
            inx_source_code: HashMap::new(),
            inx_code_target: HashMap::new(),
        }
    }

    pub fn append_exists_edge(
        &mut self,
        id: &str,
        source: &str,
        code: &str,
        target: &str,
    ) {
        let edge = Edge {
            id: id.to_string(),
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            status: 1,
        };
        self.edge_mp.insert(id.to_string(), edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            id.to_string(),
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            id.to_string(),
        );
    }

    pub fn insert_edge(&mut self, source: &str, code: &str, target: &str) -> String {
        let id = new_point();
        let edge = Edge {
            id: id.clone(),
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            status: 0,
        };
        self.edge_mp.insert(id.clone(), edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            id.clone(),
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            id.clone(),
        );
        id
    }

    pub fn insert_temp_edge(&mut self, source: &str, code: &str, target: &str) -> String {
        let id = new_point();
        let edge = Edge {
            id: id.clone(),
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            status: 1,
        };
        self.edge_mp.insert(id.clone(), edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            id.clone(),
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            id.clone(),
        );
        id
    }

    pub fn get_target(&self, source: &str, code: &str) -> Option<String> {
        match self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            Some(id_v) => {
                let edge = &self.edge_mp[id_v.last().unwrap()];
                Some(edge.target.clone())
            }
            None => None,
        }
    }

    pub fn get_target_v_unchecked(&mut self, source: &str, code: &str) -> Vec<String> {
        if let Some(id_v) = self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            let mut arr = Vec::with_capacity(id_v.len());
            for id in id_v {
                arr.push(self.edge_mp[id].target.clone());
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn get_source(&self, code: &str, target: &str) -> Option<String> {
        match self
            .inx_code_target
            .get(&(code.to_string(), target.to_string()))
        {
            Some(id_v) => Some(self.edge_mp[id_v.last().unwrap()].source.clone()),
            None => None,
        }
    }

    pub fn set_target(&mut self, source: &str, code: &str, target: &str) -> Option<String> {
        match self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            Some(id_v) => {
                let edge = self.edge_mp.get_mut(id_v.last().unwrap()).unwrap();
                edge.target = target.to_string();
                Some(edge.id.clone())
            }
            None => None,
        }
    }

    pub fn take(&mut self) -> HashMap<String, Edge> {
        self.inx_source_code.clear();
        self.inx_code_target.clear();
        take(&mut self.edge_mp)
            .into_iter()
            .filter(|(_, edge)| edge.status == 0)
            .collect()
    }
}
