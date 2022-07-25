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
        if sql == "select * from example_rowsort" {
            // Even if the order is not the same as `slt` file, sqllogictest will sort them before
            // comparing.
            return Ok("1 10 2333\n2 20 2333\n10 100 2333".into());
        }
        unimplemented!("unsupported SQL: {}", sql);
    }
}

fn main() {
    let mut tester = sqllogictest::Runner::new(FakeDB);
    tester.run_file("examples/rowsort.slt").unwrap();
}
