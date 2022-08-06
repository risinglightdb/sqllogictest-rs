use std::path::PathBuf;

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

    fn run(&mut self, _sql: &str) -> Result<String, FakeDBError> {
        Ok("Hello, world!".to_string())
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(FakeDB);
    // Validator will always return true.
    tester.with_validator(|_, _| true);

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("validator.slt");

    tester.run_file(filename).unwrap();
}