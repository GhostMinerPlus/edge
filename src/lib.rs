use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Edge {
    id: String,
    context: String,
    source: String,
    code: String,
    target: String,
}
