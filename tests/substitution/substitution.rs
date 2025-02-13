use rusty_fork::rusty_fork_test;
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
        println!("{sql}");

        let result = match sql.trim().split_once(' ') {
            Some(("select", x)) => x.to_string(),
            Some(("check", x)) => {
                if x.is_empty() {
                    return Err(FakeDBError);
                } else {
                    x.to_string()
                }
            }
            Some(("path", x)) => {
                if std::fs::metadata(x).is_err() {
                    return Err(FakeDBError);
                } else {
                    x.to_string()
                }
            }
            Some(("time", x)) => {
                let _ = x.parse::<u128>().map_err(|_| FakeDBError)?;
                x.to_string()
            }
            _ => return Err(FakeDBError),
        };

        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text],
            rows: vec![vec![result]],
        })
    }
}

// Fork a subprocess to interference with the environment variables.
rusty_fork_test! {
    #[test]
    fn test_basic() {
        std::env::set_var("MY_USERNAME", "sqllogictest");
        std::env::set_var("MY_PASSWORD", "rust");

        let ctx = sqllogictest::RunnerContext::new("fake_db".to_owned());
        let mut tester = sqllogictest::Runner::new(ctx, || async { Ok(FakeDB) });

        tester.run_file("./substitution/basic.slt").unwrap();
    }
}
