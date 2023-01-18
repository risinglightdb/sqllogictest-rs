mod extended;
mod simple;

use std::{marker::PhantomData, sync::Arc};

use tokio::task::JoinHandle;

pub type Result<T> = std::result::Result<T, tokio_postgres::Error>;

pub struct Simple;
pub struct Extended;

pub struct Postgres<P> {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
    protocol: PhantomData<P>,
}

pub type PostgresSimple = Postgres<Simple>;
pub type PostgresExtended = Postgres<Extended>;

impl<P> Postgres<P> {
    pub async fn connect(config: tokio_postgres::Config) -> Result<Self> {
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;

        let join_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Postgres connection error: {:?}", e);
            }
        });

        Ok(Self {
            client: Arc::new(client),
            join_handle,
            protocol: PhantomData,
        })
    }

    /// Returns a reference of the inner Postgres client.
    pub fn pg_client(&self) -> &tokio_postgres::Client {
        &self.client
    }
}

impl<P> Drop for Postgres<P> {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}
