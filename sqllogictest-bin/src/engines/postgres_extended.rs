use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::Type;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::{DBConfig, Result};

pub struct PostgresExtended {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
}

impl PostgresExtended {
    pub(super) async fn connect(config: &DBConfig) -> Result<Self> {
        let (client, connection) = tokio_postgres::Config::new()
            .host(&config.host)
            .port(config.port)
            .dbname(&config.db)
            .user(&config.user)
            .password(&config.pass)
            .connect(tokio_postgres::NoTls)
            .await
            .context("failed to connect to postgres")?;

        let join_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("PostgresExtended connection error: {:?}", e);
            }
        });

        Ok(Self {
            client: Arc::new(client),
            join_handle,
        })
    }
}

impl Drop for PostgresExtended {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}

macro_rules! array_process {
    ($row:ident, $output:ident, $idx:ident, $t:ty) => {
        let value: Option<Vec<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                write!($output, "{{").unwrap();
                for (i, v) in value.iter().enumerate() {
                    match v {
                        Some(v) => {
                            write!($output, "{}", v).unwrap();
                        }
                        None => {
                            write!($output, "NULL").unwrap();
                        }
                    }
                    if i < value.len() - 1 {
                        write!($output, ",").unwrap();
                    }
                }
                write!($output, "}}").unwrap();
            }
            None => {
                write!($output, "NULL").unwrap();
            }
        }
    };
}

macro_rules! single_process {
    ($row:ident, $output:ident, $idx:ident, $t:ty) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                write!($output, "{}", value).unwrap();
            }
            None => {
                write!($output, "NULL").unwrap();
            }
        }
    };
}

#[async_trait]
impl sqllogictest::AsyncDB for PostgresExtended {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        use std::fmt::Write;

        let mut output = String::new();

        let is_query_sql = {
            let lower_sql = sql.to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };
        if is_query_sql {
            let rows = self.client.query(sql, &[]).await?;
            for row in rows {
                for (idx, column) in row.columns().iter().enumerate() {
                    if idx != 0 {
                        write!(output, " ").unwrap();
                    }

                    match column.type_().clone() {
                        Type::VARCHAR | Type::TEXT => {
                            let value: Option<&str> = row.get(idx);
                            match value {
                                Some(value) => {
                                    if value.is_empty() {
                                        write!(output, "(empty)").unwrap();
                                    } else {
                                        write!(output, "{}", value).unwrap();
                                    }
                                }
                                None => {
                                    write!(output, "NULL").unwrap();
                                }
                            }
                        }
                        Type::INT2 => {
                            single_process!(row, output, idx, i16);
                        }
                        Type::INT4 => {
                            single_process!(row, output, idx, i32);
                        }
                        Type::INT8 => {
                            single_process!(row, output, idx, i64);
                        }
                        Type::BOOL => {
                            let value: Option<bool> = row.get(idx);
                            match value {
                                Some(value) => {
                                    if value {
                                        write!(output, "t").unwrap();
                                    } else {
                                        write!(output, "f").unwrap();
                                    }
                                }
                                None => {
                                    write!(output, "NULL").unwrap();
                                }
                            }
                        }
                        Type::FLOAT4 => {
                            let value: Option<f32> = row.get(idx);
                            match value {
                                Some(value) => {
                                    if value == f32::INFINITY {
                                        write!(output, "Infinity").unwrap();
                                    } else if value == f32::NEG_INFINITY {
                                        write!(output, "-Infinity").unwrap();
                                    } else {
                                        write!(output, "{}", value).unwrap();
                                    }
                                }
                                None => {
                                    write!(output, "NULL").unwrap();
                                }
                            }
                        }
                        Type::FLOAT8 => {
                            let value: Option<f64> = row.get(idx);
                            match value {
                                Some(value) => {
                                    if value == f64::INFINITY {
                                        write!(output, "Infinity").unwrap();
                                    } else if value == f64::NEG_INFINITY {
                                        write!(output, "-Infinity").unwrap();
                                    } else {
                                        write!(output, "{}", value).unwrap();
                                    }
                                }
                                None => {
                                    write!(output, "NULL").unwrap();
                                }
                            }
                        }
                        Type::NUMERIC => {
                            single_process!(row, output, idx, Decimal);
                        }
                        Type::TIMESTAMP => {
                            single_process!(row, output, idx, NaiveDateTime);
                        }
                        Type::DATE => {
                            single_process!(row, output, idx, NaiveDate);
                        }
                        Type::TIME => {
                            single_process!(row, output, idx, NaiveTime);
                        }
                        Type::INT2_ARRAY => {
                            array_process!(row, output, idx, i16);
                        }
                        Type::INT4_ARRAY => {
                            array_process!(row, output, idx, i32);
                        }
                        Type::INT8_ARRAY => {
                            array_process!(row, output, idx, i64);
                        }
                        Type::FLOAT4_ARRAY => {
                            array_process!(row, output, idx, f32);
                        }
                        Type::FLOAT8_ARRAY => {
                            array_process!(row, output, idx, f64);
                        }
                        Type::NUMERIC_ARRAY => {
                            array_process!(row, output, idx, Decimal);
                        }
                        _ => {
                            todo!("Don't support {} type now.", column.type_().name())
                        }
                    }
                }
                writeln!(output).unwrap();
            }
        } else {
            self.client.execute(sql, &[]).await?;
        }
        Ok(output)
    }

    fn engine_name(&self) -> &str {
        "postgres-extended"
    }
}
