use std::path::PathBuf;

use sqllogictest::{DBOutput, DefaultColumnType};

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
        if sql == "select read()" {
            let content = std::fs::read_to_string("/tmp/test.txt")
                .map_err(|_| FakeDBError)?
                .trim()
                .to_string();

            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![content]],
            })
        } else {
            Err(FakeDBError)
        }
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("system_command.slt");

    tester.run_file(filename).unwrap();
}
