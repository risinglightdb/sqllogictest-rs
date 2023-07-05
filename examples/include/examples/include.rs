use std::path::PathBuf;

use sqllogictest::{DBOutput, DefaultColumnType, MakeWith};

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
        unimplemented!("unsupported SQL: {}", sql);
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(MakeWith(|| FakeDB));

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("include_1.slt");

    tester.run_file(filename).unwrap();
}
