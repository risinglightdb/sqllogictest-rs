use sqllogictest::{Column, DBOutput, DefaultColumnType};

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
        let path = regex::Regex::new(r#"^select read\("(.+)"\)$"#)
            .unwrap()
            .captures(sql)
            .and_then(|c| c.get(1))
            .ok_or(FakeDBError)?
            .as_str();

        println!("{path}");

        let content = std::fs::read_to_string(path)
            .map_err(|_| FakeDBError)?
            .trim()
            .to_string();

        Ok(DBOutput::Rows {
            cols: vec![Column::anon(DefaultColumnType::Text)],
            rows: vec![vec![content]],
        })
    }
}

#[test]
fn test() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });

    if let Err(e) = tester.run_file("./system_command/system_command.slt") {
        println!("{}", e.display(true));
        panic!();
    }
}

#[test]
fn test_fail() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });

    let err = tester
        .run_file("./system_command/system_command_fail.slt")
        .unwrap_err();

    assert!(err.to_string().contains("system command failed"), "{err}");

    let err = tester
        .run_file("./system_command/system_command_fail_2.slt")
        .unwrap_err();

    assert!(
        err.to_string().contains("system command stdout mismatch"),
        "{err}"
    );
}
