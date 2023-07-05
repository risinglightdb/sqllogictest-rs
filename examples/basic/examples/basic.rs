use std::path::PathBuf;

use sqllogictest::{DBOutput, DefaultColumnType};

pub struct FakeDB;

#[derive(Debug)]
pub struct FakeDBError(String);

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
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
        Err(FakeDBError("Hey you got FakeDBError!".to_string()))
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new_once(FakeDB);

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("basic.slt");

    tester.run_file(filename).unwrap();
}
