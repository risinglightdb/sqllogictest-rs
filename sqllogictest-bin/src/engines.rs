use std::fmt::Display;
use std::process::ExitStatus;
use std::time::Duration;

use async_trait::async_trait;
use clap::ValueEnum;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use sqllogictest_engines::external::ExternalDriver;
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended, PostgresSimple};
use tokio::process::Command;

use super::{DBConfig, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum EngineType {
    Postgres,
    PostgresExtended,
    External,
}

#[derive(Clone, Debug)]
pub enum EngineConfig {
    Postgres,
    PostgresExtended,
    External(String),
}

pub(crate) enum Engines {
    Postgres(PostgresSimple),
    PostgresExtended(PostgresExtended),
    External(ExternalDriver),
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
        EngineConfig::Postgres => Engines::Postgres(
            PostgresSimple::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::PostgresExtended => Engines::PostgresExtended(
            PostgresExtended::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
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
                    .map_err(|e| EnginesError(e.into()))?,
            )
        }
    })
}

#[derive(Debug)]
pub(crate) struct EnginesError(anyhow::Error);

impl Display for EnginesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for EnginesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

macro_rules! dispatch_engines {
    ($impl:expr, $inner:ident, $body:tt) => {{
        match $impl {
            Engines::Postgres($inner) => $body,
            Engines::PostgresExtended($inner) => $body,
            Engines::External($inner) => $body,
        }
    }};
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = EnginesError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        dispatch_engines!(self, e, {
            e.run(sql)
                .await
                .map_err(|e| EnginesError(anyhow::Error::from(e)))
        })
    }

    fn engine_name(&self) -> &str {
        dispatch_engines!(self, e, { e.engine_name() })
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: std::process::Command) -> std::io::Result<ExitStatus> {
        Command::from(command).status().await
    }
}
