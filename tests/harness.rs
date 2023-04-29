use sqllogictest::{DBOutput, DefaultColumnType};

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
        write!(f, "{self:?}")
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, _sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text],
            rows: vec![vec!["I'm fake.".to_string()]],
        })
    }
}
