use sqllogictest::{DBOutput, DefaultColumnType};

sqllogictest::harness!(FakeDB::new, "slt/**/*.slt");

pub struct FakeDB {
    counter: u64,
}

impl FakeDB {
    fn new() -> Self {
        Self { counter: 0 }
    }
}

#[derive(Debug)]
pub struct FakeDBError {
    message: String,
    sql_state: Option<String>,
}

impl FakeDBError {
    fn new(message: String) -> Self {
        Self {
            message,
            sql_state: None,
        }
    }

    fn with_sql_state(message: String, sql_state: String) -> Self {
        Self {
            message,
            sql_state: Some(sql_state),
        }
    }
}

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        if sql == "select * from example_basic" {
            return Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![
                    vec!["Alice".to_string()],
                    vec!["Bob".to_string()],
                    vec!["Eve".to_string()],
                ],
            });
        }
        if sql == "select * from example_sort" {
            // Even if the order is not the same as `slt` file, sqllogictest will sort them before
            // comparing.
            return Ok(DBOutput::Rows {
                types: vec![
                    DefaultColumnType::Integer,
                    DefaultColumnType::Integer,
                    DefaultColumnType::Integer,
                ],
                rows: vec![
                    vec!["1".to_string(), "10".to_string(), "2333".to_string()],
                    vec!["2".to_string(), "20".to_string(), "2333".to_string()],
                    vec!["10".to_string(), "100".to_string(), "2333".to_string()],
                ],
            });
        }
        if sql == "select counter()" {
            self.counter += 1;
            return Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Integer],
                rows: vec![vec![self.counter.to_string()]],
            });
        }
        if sql.starts_with("create") {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.starts_with("insert") {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.starts_with("drop") {
            return Ok(DBOutput::StatementComplete(0));
        }
        if sql.starts_with("desc") {
            return Err(FakeDBError::new(
                "The operation (describe) is not supported. Did you mean [describe]?".to_string(),
            ));
        }
        if sql.contains("multiline error") {
            return Err(FakeDBError::new(
                "Hey!\n\nYou got:\n  Multiline FakeDBError!".to_string(),
            ));
        }

        // Handle SQL state error testing
        // Order matters: more specific patterns should come first
        if sql.contains("non_existent_column") || sql.contains("missing_column") {
            return Err(FakeDBError::with_sql_state(
                "column \"missing_column\" does not exist".to_string(),
                "42703".to_string(),
            ));
        }
        if sql.contains("FORM") || sql.contains("SELEKT") || sql.contains("INVALID SYNTAX") {
            return Err(FakeDBError::with_sql_state(
                "syntax error at or near \"FORM\"".to_string(),
                "42601".to_string(),
            ));
        }
        if sql.contains("1/0") || sql.contains("/0") {
            return Err(FakeDBError::with_sql_state(
                "division by zero".to_string(),
                "22012".to_string(),
            ));
        }
        if sql.contains("duplicate") || sql.contains("UNIQUE") || sql.contains("violation") {
            return Err(FakeDBError::with_sql_state(
                "duplicate key value violates unique constraint".to_string(),
                "23505".to_string(),
            ));
        }
        if sql.contains("non_existent_table")
            || sql.contains("missing_table")
            || sql.contains("table_that_does_not_exist")
            || sql.contains("final_missing_table")
            || sql.contains("query_missing_table")
            || sql.contains("another_missing_table")
            || sql.contains("yet_another_missing_table")
            || sql.contains("definitely_missing_table")
            || sql.contains("postgres_missing_table")
            || sql.contains("some_missing_table")
        {
            return Err(FakeDBError::with_sql_state(
                "relation \"missing_table\" does not exist".to_string(),
                "42P01".to_string(),
            ));
        }

        Err(FakeDBError::new("Hey you got FakeDBError!".to_string()))
    }

    fn error_sql_state(err: &Self::Error) -> Option<String> {
        err.sql_state.clone()
    }
}
