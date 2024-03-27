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
