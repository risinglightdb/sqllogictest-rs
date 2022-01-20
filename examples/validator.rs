use std::path::Path;

pub struct FakeDB;

#[derive(Debug)]
pub struct FakeDBError;

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;

    fn run(&self, _sql: &str) -> Result<String, FakeDBError> {
        Ok("Hello, world!".to_string())
    }
}

fn main() {
    let script = std::fs::read_to_string(Path::new("examples/validator.slt")).unwrap();
    let mut tester = sqllogictest::Runner::new(FakeDB);
    // Validator will always return true.
    tester.with_validator(|_, _| true);
    tester.run_script(&script).unwrap();
}
