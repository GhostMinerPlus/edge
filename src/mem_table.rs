use std::{collections::HashMap, mem::take};

fn insert(mp: &mut HashMap<(String, String), Vec<String>>, k: (String, String), v: String) {
    if let Some(uuid_v) = mp.get_mut(&k) {
        uuid_v.push(v);
    } else {
        mp.insert(k, vec![v]);
    }
}

// Public
pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Clone)]
pub struct Edge {
    pub source: String,
    pub code: String,
    pub target: String,
    is_temp: bool,
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

    pub fn append_exists_edge(&mut self, source: &str, code: &str, target: &str) {
        let uuid = new_point();
        let edge = Edge {
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            is_temp: true,
        };
        self.edge_mp.insert(uuid.to_string(), edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            uuid.to_string(),
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            uuid.to_string(),
        );
    }

    pub fn insert_edge(&mut self, source: &str, code: &str, target: &str) -> String {
        let uuid = new_point();
        let edge = Edge {
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            is_temp: false,
        };
        self.edge_mp.insert(uuid.clone(), edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            uuid.clone(),
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            uuid.clone(),
        );
        uuid
    }

    pub fn insert_temp_edge(&mut self, source: &str, code: &str, target: &str) -> String {
        let uuid = new_point();
        let edge = Edge {
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            is_temp: true,
        };
        self.edge_mp.insert(uuid.clone(), edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            uuid.clone(),
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            uuid.clone(),
        );
        uuid
    }

    pub fn get_target(&self, source: &str, code: &str) -> Option<String> {
        match self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            Some(uuid_v) => {
                let edge = &self.edge_mp[uuid_v.last().unwrap()];
                Some(edge.target.clone())
            }
            None => None,
        }
    }

    pub fn get_target_v_unchecked(&mut self, source: &str, code: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            for uuid in uuid_v {
                arr.push(self.edge_mp[uuid].target.clone());
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn get_source_v_unchecked(&mut self, code: &str, target: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_code_target
            .get(&(code.to_string(), target.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            for uuid in uuid_v {
                arr.push(self.edge_mp[uuid].source.clone());
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
            Some(uuid_v) => Some(self.edge_mp[uuid_v.last().unwrap()].source.clone()),
            None => None,
        }
    }

    pub fn take(&mut self) -> HashMap<String, Edge> {
        self.inx_source_code.clear();
        self.inx_code_target.clear();
        take(&mut self.edge_mp)
            .into_iter()
            .filter(|(_, edge)| !edge.is_temp)
            .collect()
    }

    pub fn take_some(&mut self) -> HashMap<String, Edge> {
        let edge_mp: HashMap<String, Edge> = self
            .edge_mp
            .clone()
            .into_iter()
            .filter(|(_, edge)| edge.is_temp)
            .collect();

        self.inx_source_code.clear();
        self.inx_code_target.clear();
        let r = take(&mut self.edge_mp)
            .into_iter()
            .filter(|(_, edge)| !edge.is_temp)
            .collect();

        for (_, edge) in &edge_mp {
            self.insert_temp_edge(&edge.source, &edge.code, &edge.target);
        }

        r
    }

    pub fn delete_edge_with_source_code(&mut self, source: &str, code: &str) {
        if let Some(uuid_v) = self
            .inx_source_code
            .remove(&(source.to_string(), code.to_string()))
        {
            for uuid in &uuid_v {
                let edge = self.edge_mp.remove(uuid).unwrap();
                self.inx_code_target.remove(&(edge.code, edge.target));
            }
        }
    }
}
