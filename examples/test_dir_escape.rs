pub struct FakeDB;

#[derive(Debug)]
pub struct FakeDBError;

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;

    fn run(&mut self, sql: &str) -> Result<String, FakeDBError> {
        // Output will be: sqllogictests yields copy test to '/tmp/.tmp6xSyMa/test.csv';
        println!("sqllogictests yields {}", sql);
        assert!(!sql.contains("__TEST_DIR__"));
        Ok("".into())
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(FakeDB);
    // enable `__TEST_DIR__` override
    tester.enable_testdir();
    tester.run_file("examples/test_dir_escape.slt").unwrap();
}
