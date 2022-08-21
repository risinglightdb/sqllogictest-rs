sqllogictest::harness!(FakeDB::new, "slt/**/*.slt");

pub struct FakeDB;

impl FakeDB {
    fn new() -> Self {
        Self
    }
}

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
        Ok("I'm fake.".into())
    }
}
