use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Edge {
    pub id: String,
    pub source: String,
    pub code: String,
    pub target: String,
}
