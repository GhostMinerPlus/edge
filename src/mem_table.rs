// Public
pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}
