use std::path::PathBuf;

use sqllogictest::{ColumnType, DBOutput};

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

    fn run(&mut self, sql: &str) -> Result<DBOutput, FakeDBError> {
        if sql == "select * from example_rowsort" {
            // Even if the order is not the same as `slt` file, sqllogictest will sort them before
            // comparing.
            return Ok(DBOutput::Rows {
                types: vec![
                    ColumnType::Integer,
                    ColumnType::Integer,
                    ColumnType::Integer,
                ],
                rows: vec![
                    vec!["1".to_string(), "10".to_string(), "2333".to_string()],
                    vec!["2".to_string(), "20".to_string(), "2333".to_string()],
                    vec!["10".to_string(), "100".to_string(), "2333".to_string()],
                ],
            });
        }
        unimplemented!("unsupported SQL: {}", sql);
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(FakeDB);

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("rowsort.slt");

    tester.run_file(filename).unwrap();
}
