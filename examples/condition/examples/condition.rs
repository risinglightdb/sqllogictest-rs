use std::path::PathBuf;

use sqllogictest::{ColumnType, DBOutput};

pub struct FakeDB {
    engine_name: &'static str,
}

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

    fn run(&mut self, sql: &str) -> Result<DBOutput, FakeDBError> {
        if sql.contains(self.engine_name) {
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Text],
                rows: vec![
                    vec!["Alice".to_string()],
                    vec!["Bob".to_string()],
                    vec!["Eve".to_string()],
                ],
            })
        } else {
            Err(FakeDBError)
        }
    }

    fn engine_name(&self) -> &str {
        self.engine_name
    }
}

fn main() {
    for engine_name in ["risinglight", "otherdb"] {
        let mut tester = sqllogictest::Runner::new(FakeDB { engine_name });

        let mut filename = PathBuf::from(file!());
        filename.pop();
        filename.pop();
        filename.push("condition.slt");

        tester.run_file(filename).unwrap();
    }
}
