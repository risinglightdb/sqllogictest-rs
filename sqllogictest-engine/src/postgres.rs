use std::sync::Arc;

use async_trait::async_trait;
use sqllogictest::{ColumnType, DBOutput};
use tokio::task::JoinHandle;

use crate::DBConfig;

pub type Result<T> = std::result::Result<T, tokio_postgres::Error>;

pub struct Postgres {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
}

impl Postgres {
    pub async fn connect(config: &DBConfig) -> Result<Self> {
        let (host, port) = config.random_addr();

        let (client, connection) = tokio_postgres::Config::new()
            .host(host)
            .port(port)
            .dbname(&config.db)
            .user(&config.user)
            .password(&config.pass)
            .connect(tokio_postgres::NoTls)
            .await?;

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

    /// Returns a reference of the inner Postgres client.
    pub fn pg_client(&self) -> &tokio_postgres::Client {
        &self.client
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

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        let mut output = vec![];

        // NOTE:
        // We use `simple_query` API which returns the query results as strings.
        // This means that we can not reformat values based on their type,
        // and we have to follow the format given by the specific database (pg).
        // For example, postgres will output `t` as true and `f` as false,
        // thus we have to write `t`/`f` in the expected results.
        let rows = self.client.simple_query(sql).await?;
        let mut cnt = 0;
        for row in rows {
            let mut row_vec = vec![];
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_vec.push("(empty)".to_string());
                                } else {
                                    row_vec.push(v.to_string());
                                }
                            }
                            None => row_vec.push("NULL".to_string()),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(cnt_) => {
                    cnt = cnt_;
                    break;
                }
                _ => unreachable!(),
            }
            output.push(row_vec);
        }

        if output.is_empty() {
            Ok(DBOutput::StatementComplete(cnt))
        } else {
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "postgres"
    }
}
