# Sqllogictest-rs

[![Crate](https://img.shields.io/crates/v/sqllogictest.svg)](https://crates.io/crates/sqllogictest)
[![Docs](https://docs.rs/sqllogictest/badge.svg)](https://docs.rs/sqllogictest)
[![CI](https://github.com/risinglightdb/sqllogictest-rs/workflows/CI/badge.svg?branch=main)](https://github.com/risinglightdb/sqllogictest-rs/actions)

[Sqllogictest][Sqllogictest] is a testing framework to verify the correctness of an SQL database.

This repository provides two crates:
- `sqllogictest` is a library containing sqllogictest parser and runner.
- `sqllogictest-bin` is a CLI tool to run sqllogictests.

[Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

## Use the library

Refer to the [rustdoc](https://docs.rs/sqllogictest/latest/sqllogictest/). 

## Use the CLI tool

The CLI tool supports many useful features:
- Colorful diff output
- Automatically update test files according to the actual output
- JUnit format test result report
- Parallel execution isolated with different databases
- ...

To install the binary:

```sh
cargo install sqllogictest-bin
```

You can use it as follows:

```sh
# run scripts in `test` directory against postgres with default connection settings
sqllogictest './test/**/*.slt'
# run the tests, and update the test files with the actual output!
sqllogictest './test/**/*.slt' --override
```

You can find more options in `sqllogictest --help` .

> **Note**
>
> Currently only postgres and mysql are supported in the CLI tool.

## `.slt` Test File Format Cookbook

Test files often have the `.slt` extension and use a dialect of Sqlite [Sqllogictest].

Some commonly used features of `sqlparser-rs` are show below, and many more
are illustrated in the files in the [tests](./tests) directory.

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

### Extension: Run a query/statement that should fail with the expacted error message

The syntax:
- Do not check the error message: `[statement|query] error`
- Single line error message (regexp match): `[statement|query] error <regex>`
- Multiline error message (exact match): Use `----`.

```text
# Ensure that the statement errors and that the error
# message contains 'Multiple object drop not supported'
statement error Multiple object drop not supported
DROP VIEW foo, bar;

# The output error message must be the exact match of the expected one to pass the test,
# except for the leading and trailing whitespaces.
# Empty lines (not consecutive) are allowed in the expected error message. As a result, the message must end with 2 consecutive empty lines.
query error
SELECT 1/0;
----
db error: ERROR: Failed to execute query

Caused by these errors:
1: Failed to evaluate expression: 1/0
2: Division by zero


# The next record begins here after 2 blank lines.
```

### Extension: Run external shell commands

This is useful for manipulating some external resources during the test.

```text
system ok
exit 0

# The runner will check the exit code of the command, and this will fail.
system ok
exit 1

# Check the output of the command. Same as `error`, empty lines (not consecutive) are allowed, and 2 consecutive empty lines ends the result.
system ok
echo $'Hello\n\nWorld'
----
Hello

World


# The next record begins here after 2 blank lines.

# Environment variables are supported.
system ok
echo $USER
----
xxchan
```

### Extension: Retry

```text
query I retry 3 backoff 5s
SELECT id FROM test;
----
1

query error retry 3 backoff 5s
SELECT id FROM test;
----
database error: table not found


statement ok retry 3 backoff 5s
UPDATE test SET id = 1;

statement error
UPDATE test SET value = value + 1; 
----
database error: table not found
```

Due to the limitation of syntax, the retry clause can't be used along with the single-line regex error message extension.

### Extension: Environment variable substitution in query and statement

It needs to be enabled by adding `control substitution on` to the test file.

```
control substitution on

# see https://docs.rs/subst/latest/subst/ for all features
query TTTT
SELECT
  '$foo'                -- short
, '${foo}'              -- long
, '${bar:default}'      -- default value
, '${bar:$foo-default}' -- recursive default value
FROM baz;
----
...
```

Besides, there're some special variables supported:
- `$__TEST_DIR__`: the path to a temporary directory specific to the current test case. 
  This can be helpful if you need to manipulate some external resources during the test.
- `$__NOW__`: the current Unix timestamp in nanoseconds.

```
control substitution on

statement ok
COPY (SELECT * FROM foo) TO '$__TEST_DIR__/foo.txt';

system ok
echo "foo" > "$__TEST_DIR__/foo.txt"
```

> [!NOTE]
>
> When substitution is on, special characters need to be escaped, e.g., `\$` and `\\`.
>
> `system` commands don't support the advanced substitution features of the [subst](https://docs.rs/subst/latest/subst/) crate,
> and excaping is also not needed.
> Environment variables are supported by the shell, and special variables are still supported by plain string substitution.

## Used by

- [RisingLight](https://github.com/risinglightdb/risinglight): An OLAP database system for educational purpose
- [RisingWave](https://github.com/risingwavelabs/risingwave): The next-generation streaming database in the cloud
- [DataFusion](https://github.com/apache/arrow-datafusion): Apache Arrow DataFusion SQL Query Engine
- [Databend](https://github.com/datafuselabs/databend): A powerful cloud data warehouse
- [CnosDB](https://github.com/cnosdb/cnosdb): Open Source Distributed Time Series Database

## Contributing

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

Contributors should add a Signed-off-by line for [Developer Certificate of Origin](https://github.com/probot/dco#how-it-works)
in their commits. Use `git commit -s` to sign off commits.

## License

This project is available under the terms of either the [Apache 2.0 license](LICENSE-APACHE) or the [MIT license](LICENSE-MIT).
