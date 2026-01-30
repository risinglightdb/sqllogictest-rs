use sqllogictest::{DBOutput, DefaultColumnType};

pub struct FakeDB;

#[derive(Debug)]
pub struct FakeDBError(String);

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        println!("[SQL] {sql}");

        let parts: Vec<&str> = sql.split_whitespace().collect();
        match parts.as_slice() {
            // select_id <value> -> returns a single row with one column
            ["select_id", value] => Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![value.to_string()]],
            }),
            // select_pair <name> <value> -> returns a single row with two columns
            ["select_pair", name, value] => Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text, DefaultColumnType::Text],
                rows: vec![vec![name.to_string(), value.to_string()]],
            }),
            // select <value> -> returns the value
            ["select", value] => Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![value.to_string()]],
            }),
            // select <v1>, <v2> -> returns two values
            ["select", v1, v2] => {
                // Remove trailing comma from v1 if present
                let v1 = v1.trim_end_matches(',');
                Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Text, DefaultColumnType::Text],
                    rows: vec![vec![v1.to_string(), v2.to_string()]],
                })
            }
            // echo <value> -> returns the value (for testing variable substitution)
            ["echo", value] => Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![value.to_string()]],
            }),
            // select_rows <n> -> returns n rows (for testing row count validation)
            ["select_rows", n] => {
                let n: usize = n.parse().map_err(|_| FakeDBError("invalid number".into()))?;
                let rows = (0..n).map(|i| vec![i.to_string()]).collect();
                Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Integer],
                    rows,
                })
            }
            // statement -> returns statement complete
            ["statement"] => Ok(DBOutput::StatementComplete(0)),
            _ => Err(FakeDBError(format!("unknown command: {sql}"))),
        }
    }
}

#[test]
fn test_let_basic() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    tester.run_file("./let/basic.slt").unwrap();
}

#[test]
fn test_let_row_count_error() {
    // Test that let fails when query returns != 1 row
    let script = r#"
control substitution on

let (id)
select_rows 0
"#;

    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    let result = tester.run_script(script);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("expected 1 row, got 0"),
        "Error message should mention row count mismatch: {}",
        err
    );
}

#[test]
fn test_let_row_count_error_multiple() {
    // Test that let fails when query returns multiple rows
    let script = r#"
control substitution on

let (id)
select_rows 3
"#;

    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    let result = tester.run_script(script);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("expected 1 row, got 3"),
        "Error message should mention row count mismatch: {}",
        err
    );
}

#[test]
fn test_let_column_count_error() {
    // Test that let fails when column count doesn't match variable count
    let script = r#"
control substitution on

let (id, name)
select_id 42
"#;

    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    let result = tester.run_script(script);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("expected 2 columns, got 1"),
        "Error message should mention column count mismatch: {}",
        err
    );
}

#[test]
fn test_let_statement_error() {
    // Test that let fails when the SQL returns a statement result instead of query
    let script = r#"
control substitution on

let (id)
statement
"#;

    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    let result = tester.run_script(script);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("expected query result, got statement completion"),
        "Error message should mention statement completion: {}",
        err
    );
}
