use serde::Deserialize;

// Public
#[derive(Clone, Deserialize, Debug)]
pub struct Inc {
    pub output: String,
    pub operator: String,
    pub function: String,
    pub input: String,
    pub input1: String,
}

impl ToString for Inc {
    fn to_string(&self) -> String {
        format!(
            "{} {} {} {} {}",
            self.output, self.operator, self.function, self.input, self.input1
        )
    }
}
