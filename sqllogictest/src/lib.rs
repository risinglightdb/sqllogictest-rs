//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//!
//! # Usage
//!
//! Implement [`DB`] trait for your database structure:
//!
//! ```ignore
//! struct Database {...}
//!
//! impl sqllogictest::DB for Database {
//!     type Error = ...;
//!     fn run(&self, sql: &str) -> Result<String, Self::Error> {
//!         ...
//!     }
//! }
//! ```
//!
//! Create a [`Runner`] on your database instance, and then run the script:
//!
//! ```ignore
//! let mut tester = sqllogictest::Runner::new(Database::new());
//! let script = std::fs::read_to_string("script.slt").unwrap();
//! tester.run_script(&script);
//! ```
//!
//! You can also parse the script and execute the records separately:
//!
//! ```ignore
//! let records = sqllogictest::parse(&script).unwrap();
//! for record in records {
//!     tester.run(record);
//! }
//! ```

#[cfg(feature = "fmt")]
mod fmt;
pub mod harness;
pub mod parser;
pub mod runner;

#[cfg(feature = "fmt")]
pub use self::fmt::*;
pub use self::parser::*;
pub use self::runner::*;
