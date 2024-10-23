use sqllogictest::{strict_column_validator, Column, ColumnType, DBOutput};

pub struct FakeDB;

#[derive(Debug)]
pub struct FakeDBError;

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for FakeDBError {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CustomColumnType {
    Integer,
    Boolean,
    Date,
}

impl ColumnType for CustomColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'I' => Some(Self::Integer),
            'B' => Some(Self::Boolean),
            'D' => Some(Self::Date),
            _ => None,
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Integer => 'I',
            Self::Boolean => 'B',
            Self::Date => 'D',
        }
    }
}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;
    type ColumnType = CustomColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        if sql == "select * from example_typed" {
            Ok(DBOutput::Rows {
                cols: vec![
                    Column::anon(CustomColumnType::Integer),
                    Column::anon(CustomColumnType::Boolean),
                ],
                rows: vec![
                    vec!["1".to_string(), "true".to_string()],
                    vec!["2".to_string(), "false".to_string()],
                    vec!["3".to_string(), "true".to_string()],
                ],
            })
        } else if sql == "select * from no_results" {
            Ok(DBOutput::Rows {
                cols: vec![
                    Column::anon(CustomColumnType::Integer),
                    Column::anon(CustomColumnType::Boolean),
                ],
                rows: vec![],
            })
        } else {
            Err(FakeDBError)
        }
    }
}

#[test]
fn test() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(FakeDB) });
    tester.with_column_validator(strict_column_validator);

    tester.run_file("./custom_type/custom_type.slt").unwrap();
}
