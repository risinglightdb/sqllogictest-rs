mod postgres;
use postgres::Postgres;
mod postgres_extended;
use std::fmt::Display;

use async_trait::async_trait;
use postgres_extended::PostgresExtended;
use sqllogictest::AsyncDB;

use crate::{DBConfig, Result};

enum Engines {
    Postgres(Postgres),
    PostgresExtended(PostgresExtended),
}

pub const ENGINES: [&str; 2] = ["postgres", "postgres-extended"];

pub(super) async fn connect(engine: &str, config: &DBConfig) -> Result<impl AsyncDB> {
    Ok(match engine {
        "postgres" => Engines::Postgres(Postgres::connect(config).await?),
        "postgres-extended" => Engines::PostgresExtended(PostgresExtended::connect(config).await?),
        _ => unreachable!(),
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
    async fn run(&mut self, sql: &str) -> Result<String, anyhow::Error> {
        Ok(match self {
            Engines::Postgres(e) => e.run(sql).await?,
            Engines::PostgresExtended(e) => e.run(sql).await?,
        })
    }
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = AnyhowError;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        self.run(sql).await.map_err(AnyhowError)
    }
}
