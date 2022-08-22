use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::{DBConfig, Result};

pub struct Postgres {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
}

impl Postgres {
    pub(super) async fn connect(config: &DBConfig) -> Result<Self> {
        let (host, port) = config.random_addr();

        let (client, connection) = tokio_postgres::Config::new()
            .host(host)
            .port(port)
            .dbname(&config.db)
            .user(&config.user)
            .password(&config.pass)
            .connect(tokio_postgres::NoTls)
            .await
            .context(format!("failed to connect to postgres at {host}:{port}"))?;

        let join_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Postgres connection error: {:?}", e);
            }
        });

        Ok(Self {
            client: Arc::new(client),
            join_handle,
        })
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        use std::fmt::Write;

        let mut output = String::new();
        // NOTE:
        // We use `simple_query` API which returns the query results as strings.
        // This means that we can not reformat values based on their type,
        // and we have to follow the format given by the specific database (pg).
        // For example, postgres will output `t` as true and `f` as false,
        // thus we have to write `t`/`f` in the expected results.
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    write!(output, "(empty)").unwrap()
                                } else {
                                    write!(output, "{}", v).unwrap()
                                }
                            }
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }

    fn engine_name(&self) -> &str {
        "postgres"
    }
}
