# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A command-line tool to run scripts from file against postgres-compatible databases.
- Support `sleep` and `include` statement.

### Changed

- Add file location to the error message.
- Runner returns error type instead of panic.

## [0.1.0] - 2021-12-10

### Added

- Basic sqllogictest parser and runner.
