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
        // Output will be: sqllogictests yields copy test to '/tmp/.tmp6xSyMa/test.csv';
        println!("sqllogictests yields {sql}");
        assert!(!sql.contains("__TEST_DIR__"));
        Ok(DBOutput::StatementComplete(0))
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(FakeDB);
    // enable `__TEST_DIR__` override
    tester.enable_testdir();

    let mut filename = PathBuf::from(file!());
    filename.pop();
    filename.pop();
    filename.push("test_dir_escape.slt");

    tester.run_file(filename).unwrap();
}
