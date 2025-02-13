use sqllogictest::{DBOutput, DefaultColumnType};

sqllogictest::harness!("fake_db", FakeDB::new, "slt/**/*.slt");

pub struct FakeDB {
    counter: u64,
}

impl FakeDB {
    fn new() -> Self {
        Self { counter: 0 }
    }
}

#[derive(Debug)]
pub struct FakeDBError(String);

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        if sql == "select * from example_basic" {
            return Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![
                    vec!["Alice".to_string()],
                    vec!["Bob".to_string()],
                    vec!["Eve".to_string()],
                ],
            });
        }
        if sql == "select * from example_sort" {
            // Even if the order is not the same as `slt` file, sqllogictest will sort them before
            // comparing.
            return Ok(DBOutput::Rows {
                types: vec![
                    DefaultColumnType::Integer,
                    DefaultColumnType::Integer,
                    DefaultColumnType::Integer,
                ],
                rows: vec![
                    vec!["1".to_string(), "10".to_string(), "2333".to_string()],
                    vec!["2".to_string(), "20".to_string(), "2333".to_string()],
                    vec!["10".to_string(), "100".to_string(), "2333".to_string()],
                ],
            });
        }
        if sql == "select counter()" {
            self.counter += 1;
            return Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Integer],
                rows: vec![vec![self.counter.to_string()]],
            });
        }
        if sql.starts_with("create") {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.starts_with("insert") {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.starts_with("drop") {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.starts_with("desc") {
            return Err(FakeDBError(
                "The operation (describe) is not supported. Did you mean [describe]?".to_string(),
            ));
        }
        if sql.contains("multiline error") {
            return Err(FakeDBError(
                "Hey!\n\nYou got:\n  Multiline FakeDBError!".to_string(),
            ));
        }
        Err(FakeDBError("Hey you got FakeDBError!".to_string()))
    }
}
