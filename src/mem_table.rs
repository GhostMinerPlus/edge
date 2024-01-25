// Public
pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub struct Edge {
    id: String,
    source: String,
    code: String,
    no: u64,
    target: String,
}

pub struct MemTable {
    edge_v: Vec<Edge>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            edge_v: Vec::new(),
        }
    }
}
