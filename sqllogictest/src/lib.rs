//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! This crate supports multiple extensions beyond the original sqllogictest format.
//! See the [README](https://github.com/risinglightdb/sqllogictest-rs#slt-test-file-format-cookbook) for more information.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//!
//! # Usage
//!
//! For how to use the CLI tool backed by this library, see the [README](https://github.com/risinglightdb/sqllogictest-rs#use-the-cli-tool).
//!
//! For using the crate as a lib, and implement your custom driver, see below.
//!
//! Implement [`DB`] trait for your database structure:
//!
//! ```ignore
//! struct Database {...}
//!
//! impl sqllogictest::DB for Database {
//!     type Error = ...;
//!     type ColumnType = ...;
//!     fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
//!         ...
//!     }
//! }
//! ```
//!
//! Then create a `Runner` on your database instance, and run the tests:
//!
//! ```ignore
//! let db = Database {...};
//! let mut tester = sqllogictest::Runner::new(db);
//! tester.run_file("script.slt").unwrap();
//! ```
//!
//! You can also parse the script and execute the records separately:
//!
//! ```rust
//! let records = sqllogictest::parse_file("script.slt").unwrap();
//! for record in records {
//!     tester.run(record).unwrap();
//! }
//! ```

pub mod column_type;
pub mod connection;
pub mod harness;
pub mod parser;
pub mod runner;

pub use self::column_type::*;
pub use self::connection::*;
pub use self::parser::*;
pub use self::runner::*;

mod substitution;
