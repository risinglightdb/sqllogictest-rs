use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use sqllogictest::{DBOutput, DefaultColumnType};
use sqlx::{mysql::MySqlPoolOptions, Column, Row, TypeInfo};

type Result<T> = std::result::Result<T, sqlx::Error>;

/// Connection configuration. This is a re-export of [`sqlx::mysql::MySqlConnectOptions`].
pub type MySqlConfig = sqlx::mysql::MySqlConnectOptions;

/// MySQL engine based on the client from [`sqlx::mysql`].
pub struct MySql {
    pool: sqlx::Pool<sqlx::MySql>,
}

impl MySql {
    /// Connects to the MySQL server with the given `config`.
    pub async fn connect(config: MySqlConfig) -> Result<Self> {
        let pool = MySqlPoolOptions::new().connect_with(config).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for MySql {
    type Error = sqlx::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        // NOTE:
        // Only certain data types in MySQL are supported. If there is an
        // unsupported data type in the row, a decode error will be returned.
        if sql.trim_start().to_lowercase().starts_with("select") {
            let mut output = vec![];
            let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
            for row in rows {
                let mut row_vec = vec![];
                for i in 0..row.len() {
                    let type_info = row.column(i).type_info();
                    // Column type naming here is aligned with sqlx.
                    // Refer: https://github.com/launchbadge/sqlx/blob/main/sqlx-mysql/src/protocol/text/column.rs#L169
                    let value = match type_info.name() {
                        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                            row.try_get::<String, _>(i)
                        }
                        "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" | "BIGINT" => {
                            row.try_get::<i64, _>(i).map(|v| v.to_string())
                        }
                        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED"
                        | "MEDIUMINT UNSIGNED" | "BIGINT UNSIGNED" => {
                            row.try_get::<u64, _>(i).map(|v| v.to_string())
                        }
                        "FLOAT" | "DOUBLE" | "DECIMAL" => {
                            row.try_get::<f64, _>(i).map(|v| v.to_string())
                        }
                        _ => {
                            return Err(sqlx::Error::Decode(
                                format!("Unsupported type: {}", type_info.name()).into(),
                            ))
                        }
                    };
                    let value = match value {
                        Ok(v) => {
                            if v.is_empty() {
                                "(empty)".to_string()
                            } else {
                                v
                            }
                        }
                        Err(_) => "NULL".to_string(),
                    };
                    row_vec.push(value);
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
            let result = sqlx::query(sql).execute(&self.pool).await?;
            Ok(DBOutput::StatementComplete(result.rows_affected()))
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
