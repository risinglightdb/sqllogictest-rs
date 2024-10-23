use sqllogictest::{Column, DBOutput, DefaultColumnType};

pub struct FakeDB;

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
            cols: vec![Column::new("c1", DefaultColumnType::Text)],
            rows: vec![vec!["Hello, world!".to_string()]],
        })
    }
}

#[test]
fn test() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    // Validator will always return true.
    tester.with_validator(|_, _| true);

    tester.run_file("./validator/validator.slt").unwrap();
}
