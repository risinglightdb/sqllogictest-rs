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

#[test]
fn test() {
    let ctx = sqllogictest::RunnerContext::new("fake_db".to_owned());
    let mut tester = sqllogictest::Runner::new(ctx, || async { Ok(FakeDB) });

    tester
        .run_file("./test_dir_escape/test_dir_escape.slt")
        .unwrap();
}
