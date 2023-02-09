use std::fmt::Display;

use async_trait::async_trait;
use clap::ArgEnum;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use sqllogictest_engines::external::ExternalDriver;
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended, PostgresSimple};
use tokio::process::Command;

use super::{DBConfig, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ArgEnum)]
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

enum Engines {
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

        pg_config
    }
}

pub(super) async fn connect(engine: &EngineConfig, config: &DBConfig) -> Result<impl AsyncDB> {
    Ok(match engine {
        EngineConfig::Postgres => Engines::Postgres(PostgresSimple::connect(config.into()).await?),
        EngineConfig::PostgresExtended => {
            Engines::PostgresExtended(PostgresExtended::connect(config.into()).await?)
        }
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
            Engines::External(ExternalDriver::connect(cmd).await?)
        }
    })
}

#[derive(Debug)]
struct AnyhowError(anyhow::Error);

impl Display for AnyhowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for AnyhowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl Engines {
    async fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, anyhow::Error> {
        Ok(match self {
            Engines::Postgres(e) => e.run(sql).await?,
            Engines::PostgresExtended(e) => e.run(sql).await?,
            Engines::External(e) => e.run(sql).await?,
        })
    }
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = AnyhowError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.run(sql).await.map_err(AnyhowError)
    }
}
