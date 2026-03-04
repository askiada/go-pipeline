# Changelog

All notable changes to this project will be documented in this file.
This format is based on Keep a Changelog, and this project uses Semantic Versioning.

## Unreleased

### Added
- More Go doc examples for split/merge and error routing.
- Package doc examples for drawer, model, and monitor.

### Changed
- README quick start now matches the Go doc example.
- Go doc examples now use range loops and clearer spacing.

## v2.0.0

### Added
- Core pipeline steps: root, one-to-one, one-to-many, from-chan, sink.
- Splitters and mergers for fan-out and fan-in.
- Step options for concurrency, buffering, retries, drops, timeouts, and rate limits.
- Metrics and drawer options.
- Live monitoring option.
- Examples and docs set.
