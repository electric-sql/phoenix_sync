# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.3] - 2025-05-20

### Changed

- Updated to support latest Electric version (v1.0.13)

## [0.4.2] - 2025-05-14

### Fixed

- Correctly resolve sync macro Plug within aliased `Phoenix.Router` scope ([#40](https://github.com/electric-sql/phoenix_sync/pull/40)).

## [0.4.1] - 2025-05-14

### Fixed

- Embedded client includes correct `content-type` headers ([#35](https://github.com/electric-sql/phoenix_sync/pull/35)).
- Server errors are now propagated to the Liveview so they are not obscured by errors due to missing resume message ([#37](https://github.com/electric-sql/phoenix_sync/pull/37)).
- Credentials and other configured params are now correctly included in the `Electric.Client` configuration ([#38](https://github.com/electric-sql/phoenix_sync/pull/38)).

## [0.4.0] - 2025-05-13

### Added

- `Phoenix.Sync.Writer` for handling optimistic writes in the client

## [0.3.4] - 2025-03-25

### Changed

- Updated to support Electric v1.0.1
