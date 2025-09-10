use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use clap::ValueEnum;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use sqllogictest_engines::external::ExternalDriver;
use sqllogictest_engines::mysql::{MySql, MySqlConfig};
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended, PostgresSimple};
use tokio::process::Command;

use super::{DBConfig, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum EngineType {
    Mysql,
    Postgres,
    PostgresExtended,
    External,
}

#[derive(Clone, Debug)]
pub enum EngineConfig {
    MySql,
    Postgres,
    PostgresExtended,
    External(String),
}

pub(crate) enum Engines {
    MySql(MySql),
    Postgres(PostgresSimple),
    PostgresExtended(PostgresExtended),
    External(ExternalDriver),
}

impl From<&DBConfig> for MySqlConfig {
    fn from(config: &DBConfig) -> Self {
        let (host, port) = config.random_addr();
        let database_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.user, config.pass, host, port, config.db
        );

        MySqlConfig::from_url(&database_url).unwrap()
    }
}

impl From<&DBConfig> for PostgresConfig {
    fn from(config: &DBConfig) -> Self {
        let (host, port) = config.random_addr();

        let mut pg_config = PostgresConfig::new();
        pg_config
            .host(host)
            .port(port)
            .dbname(&config.db)
            .user(&config.user)
            .password(&config.pass);
        if let Some(options) = &config.options {
            pg_config.options(options);
        }

        pg_config
    }
}

pub(crate) async fn connect(
    engine: &EngineConfig,
    config: &DBConfig,
) -> Result<Engines, EnginesError> {
    Ok(match engine {
        EngineConfig::MySql => Engines::MySql(
            MySql::connect(config.into())
                .await
                .map_err(EnginesError::without_state)?,
        ),
        EngineConfig::Postgres => Engines::Postgres(
            PostgresSimple::connect(config.into())
                .await
                .map_err(EnginesError::without_state)?,
        ),
        EngineConfig::PostgresExtended => Engines::PostgresExtended(
            PostgresExtended::connect(config.into())
                .await
                .map_err(EnginesError::without_state)?,
        ),
        EngineConfig::External(cmd_tmpl) => {
            let (host, port) = config.random_addr();
            let cmd_str = cmd_tmpl
                .replace("{db}", &config.db)
                .replace("{host}", host)
                .replace("{port}", &port.to_string())
                .replace("{user}", &config.user)
                .replace("{pass}", &config.pass);
            let mut cmd = Command::new("bash");
            cmd.args(["-c", &cmd_str]);
            Engines::External(
                ExternalDriver::connect(cmd)
                    .await
                    .map_err(EnginesError::without_state)?,
            )
        }
    })
}

#[derive(Debug)]
pub(crate) struct EnginesError {
    error: anyhow::Error,
    sqlstate: Option<String>,
}

impl EnginesError {
    fn without_state(error: impl Into<anyhow::Error>) -> Self {
        Self {
            error: error.into(),
            sqlstate: None,
        }
    }
}

impl Display for EnginesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for EnginesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

macro_rules! dispatch_engines {
    ($impl:expr, $inner:ident, $body:tt) => {{
        match $impl {
            Engines::MySql($inner) => $body,
            Engines::Postgres($inner) => $body,
            Engines::PostgresExtended($inner) => $body,
            Engines::External($inner) => $body,
        }
    }};
}

fn error_sql_state<E: AsyncDB>(_engine: &E, error: &E::Error) -> Option<String> {
    E::error_sql_state(error)
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = EnginesError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        dispatch_engines!(self, e, {
            e.run(sql).await.map_err(|error| EnginesError {
                sqlstate: error_sql_state(e, &error),
                error: anyhow::Error::from(error),
            })
        })
    }

    fn engine_name(&self) -> &str {
        dispatch_engines!(self, e, { e.engine_name() })
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: std::process::Command) -> std::io::Result<std::process::Output> {
        Command::from(command).output().await
    }

    async fn shutdown(&mut self) {
        dispatch_engines!(self, e, { e.shutdown().await })
    }

    fn error_sql_state(err: &Self::Error) -> Option<String> {
        err.sqlstate.clone()
    }
}
