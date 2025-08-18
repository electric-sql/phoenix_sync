# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.1] - 2025-08-18

### Changed

- Update `electric` to `~> 1.1` including new [faster storage engine](https://electric-sql.com/blog/2025/08/13/electricsql-v1.1-released).
- Update `electric_client` to `~> 0.7.0` which includes `txid` headers in sync messages.

### Fixed

- Use 32-bit txid to ensure consistency with Electric sync messages ([#71](https://github.com/electric-sql/phoenix_sync/pull/71))
- Only enable sandbox if both `Ecto.SQL` and `Electric` are installed ([#86](https://github.com/electric-sql/phoenix_sync/pull/86))
- Fix liveview startup [#87](https://github.com/electric-sql/phoenix_sync/issues/87) ([#88](https://github.com/electric-sql/phoenix_sync/pull/88))
- Fix occasional compilation error when `Electric` installed ([#89](https://github.com/electric-sql/phoenix_sync/pull/89))

## [0.5.0] - 2025-08-13

### Added

- A new [`Phoenix.Sync.Shape`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Shape.html) that maintains an live, in-memory representation of the current state of the database ([#77](https://github.com/electric-sql/phoenix_sync/pull/77))
- Integration with `Ecto.Adapters.SQL.Sandbox` via [`Phoenix.Sync.Sandbox`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Sandbox.html) to enable simulating updates to shapes within a test transaction ([#73](https://github.com/electric-sql/phoenix_sync/pull/73))
- Interruptible shape endpoints using [`Phoenix.Sync.Controller.sync_render/3`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Controller.html#sync_render/3) ([#65](https://github.com/electric-sql/phoenix_sync/pull/65))
- Support for defining shapes via a `changeset/1` function ([#70](https://github.com/electric-sql/phoenix_sync/pull/70))

### Fixed

- Improve error messages caused by invalid module names ([#74](https://github.com/electric-sql/phoenix_sync/pull/74))
- Fix compilation errors when included with no `:ecto` dependency ([#79](https://github.com/electric-sql/phoenix_sync/pull/79))

## [0.4.4] - 2025-06-30

### Added

- Both controller- and router-based shapes now return CORS headers for all requests requests ([#62](https://github.com/electric-sql/phoenix_sync/pull/62))

### Changed

- Updated `electric_client` to support `Ecto.ULID`, `:map` and `:array` types ([v0.6.3](https://github.com/electric-sql/electric/releases/tag/%40core%2Felixir-client%400.6.3))
- Updated `electric` to latest version ([v1.0.21](https://github.com/electric-sql/electric/releases/tag/%40core%2Fsync-service%401.0.21))

### Fixed

- `Phoenix.Sync` will now emit a warning if the configuration is missing the `env` setting ([#60](https://github.com/electric-sql/phoenix_sync/pull/60))

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
