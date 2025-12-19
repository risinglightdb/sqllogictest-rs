use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::Value;
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
        let mut output = vec![];
        let rows: Vec<mysql_async::Row> = conn.query(sql).await?;
        for row in rows {
            let mut row_vec = vec![];
            for i in 0..row.len() {
                // Since `query*` API in `mysql_async` is implemented using the MySQL text protocol,
                // we can assume that the return value will be of type `Value::Bytes` or
                // `Value::NULL`.
                let value = row[i].clone();
                let value_str = match value {
                    Value::Bytes(bytes) => match String::from_utf8(bytes) {
                        Ok(x) => x,
                        Err(_) => unreachable!(),
                    },
                    Value::NULL => "NULL".to_string(),
                    _ => unreachable!(),
                };
                if value_str.is_empty() {
                    row_vec.push("(empty)".to_string());
                } else {
                    row_vec.push(value_str);
                }
            }
            output.push(row_vec);
        }
        if output.is_empty() {
            Ok(DBOutput::StatementComplete(conn.affected_rows()))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    async fn shutdown(&mut self) {
        self.pool.clone().disconnect().await.ok();
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

    fn error_sql_state(err: &Self::Error) -> Option<String> {
        if let mysql_async::Error::Server(err) = err {
            Some(err.state.clone())
        } else {
            None
        }
    }
}
