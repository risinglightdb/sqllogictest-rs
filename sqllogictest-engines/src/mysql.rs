use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::FromValue;
use mysql_async::prelude::Queryable;
use mysql_async::FromValueError;
use sqllogictest::{DBOutput, DefaultColumnType};

type Result<T> = std::result::Result<T, mysql_async::Error>;

/// Connection configuration. This is a re-export of [`mysql_async::Opts`].
pub type MySqlConfig = mysql_async::Opts;

/// MySQL engine based on the client from [`mysql_async`].
pub struct MySql {
    pool: mysql_async::Pool,
}

impl MySql {
    /// Connects to the MySQL server with the given `config`.
    pub async fn connect(config: MySqlConfig) -> Result<Self> {
        let pool = mysql_async::Pool::new(config);
        Ok(Self { pool })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for MySql {
    type Error = mysql_async::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        let mut conn = self.pool.get_conn().await?;
        if sql.trim_start().to_lowercase().starts_with("select") {
            let mut output = vec![];
            let rows: Vec<mysql_async::Row> = conn.query(sql).await?;
            for row in rows {
                let mut row_vec = vec![];
                for i in 0..row.len() {
                    let value: std::result::Result<String, FromValueError> =
                        FromValue::from_value_opt(row[i].clone());
                    match value {
                        Ok(value) => {
                            if value.is_empty() {
                                row_vec.push("(empty)".to_string());
                            } else {
                                row_vec.push(value);
                            }
                        }
                        Err(_) => {
                            row_vec.push("NULL".to_string());
                        }
                    }
                }
                output.push(row_vec);
            }
            if output.is_empty() {
                Ok(DBOutput::StatementComplete(0))
            } else {
                Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Any; output[0].len()],
                    rows: output,
                })
            }
        } else {
            let rows: Vec<mysql_async::Row> = conn.exec(sql, ()).await?;
            Ok(DBOutput::StatementComplete(rows.len() as u64))
        }
    }

    fn engine_name(&self) -> &str {
        "mysql"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: Command) -> std::io::Result<std::process::Output> {
        tokio::process::Command::from(command).output().await
    }
}
