use std::path::PathBuf;

use sqllogictest::{DBOutput, DefaultColumnType};

pub struct FakeDB {
    counter: u64,
}

impl FakeDB {
    #[allow(clippy::unused_async)]
    async fn connect() -> Result<Self, FakeDBError> {
        Ok(Self { counter: 0 })
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

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        if sql == "select counter()" {
            self.counter += 1;
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Integer],
                rows: vec![vec![self.counter.to_string()]],
            })
        } else {
            Err(FakeDBError)
        }
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(FakeDB::connect);

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("connection.slt");

    tester.run_file(filename).unwrap();
}
