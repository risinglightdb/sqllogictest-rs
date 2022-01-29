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

    fn run(&mut self, sql: &str) -> Result<String, FakeDBError> {
        if sql == "select * from example_basic" {
            return Ok("Alice\nBob\nEve".into());
        }
        if sql.starts_with("create") {
            return Ok("".into());
        }
        if sql.starts_with("insert") {
            return Ok("".into());
        }
        if sql.starts_with("drop") {
            return Ok("".into());
        }
        unimplemented!("unsupported SQL: {}", sql);
    }
}

fn main() {
    let script = std::fs::read_to_string(Path::new("examples/basic.slt")).unwrap();
    let mut tester = sqllogictest::Runner::new(FakeDB);
    tester.run_script(&script).unwrap();
}
