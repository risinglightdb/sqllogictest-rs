# Sqllogictest-rs

[![Crate](https://img.shields.io/crates/v/sqllogictest.svg)](https://crates.io/crates/sqllogictest)
[![Docs](https://docs.rs/sqllogictest/badge.svg)](https://docs.rs/sqllogictest)
[![CI](https://github.com/singularity-data/sqllogictest-rs/workflows/CI/badge.svg?branch=main)](https://github.com/singularity-data/sqllogictest-rs/actions)

[Sqllogictest][Sqllogictest] is a testing framework to verify the correctness of an SQL database.

This crate implements a sqllogictest parser and runner in Rust.

[Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

## Using as Library

Add the following lines to your `Cargo.toml` file:

```toml
[dependencies]
sqllogictest = "0.9"
```

Implement `DB` trait for your database structure:

```rust
struct Database {...}

impl sqllogictest::DB for Database {
    type Error = ...;
    fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        ...
    }
}
```

It should take an SQL query string as input, and output the query result as a string.
The runner verifies the results by comparing the string after normalization.

Finally, create a `Runner` on your database instance, and then run the script:

```rust
let mut tester = sqllogictest::Runner::new(Database::new());
let script = std::fs::read_to_string("script.slt").unwrap();
tester.run_script(&script);
```

You can also parse the script and execute the records separately:

```rust
let records = sqllogictest::parse(&script).unwrap();
for record in records {
    tester.run(record);
}
```


## Using as CLI

This crate can also be used as a command-line tool.

To install the binary, the `bin` feature is required:

```sh
cargo install sqllogictest-bin
```

You can use it as follows:

```sh
sqllogictest './test/**/*.slt'
```

This command will run scripts in `test` directory against postgres with default connection settings.

You can find more options in `sqllogictest --help`.

Note that only postgres is supported now.

## `.slt` Test File Format Cookbook

Test files often have the `.slt` extension and use a dialect of Sqlite [Sqllogictest].

Some commonly used features of `sqlparser-rs` are show below, and many more
are illustrated in the files in the [examples](./examples) directory.

### Run a statement that should succeed

```text
# Comments begin with '#'
statement ok
CREATE TABLE foo AS VALUES(1,2),(2,3);
```

### Run a query that should succeed

```text
# 'II' means two integer output columns
# rowsort means to sort the output before comparing
query II rowsort
SELECT * FROM foo;
----
3 4
4 5
```

### Run a statement that should fail

```text
# Ensure that the statement errors and that the error
# message contains 'Multiple object drop not supported'
statement error Multiple object drop not supported
DROP VIEW foo, bar;
```


## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

Contributors should add a Signed-off-by line for [Developer Certificate of Origin](https://github.com/probot/dco#how-it-works)
in their commits. Use `git commit -s` to sign off commits.

## Publish the crate

```
cargo publish -p sqllogictest
cargo publish -p sqllogictest-bin
```
