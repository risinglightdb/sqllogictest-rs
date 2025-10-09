use std::error::Error;

#[derive(Debug)]
pub struct PgDriverError(tokio_postgres::Error);

impl std::fmt::Display for PgDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        if let Some(cause) = self.0.source() {
            write!(f, ": {}", cause)?;
        }
        Ok(())
    }
}

impl From<tokio_postgres::Error> for PgDriverError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self(value)
    }
}

impl std::error::Error for PgDriverError {}
