# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.2] - 2026-01-06

### Changed

- **Breaking**: Updated `electric` dependency to `~> 1.2.4` (dropping support for Electric 1.1.x)
- **Breaking**: Updated `electric_client` dependency to `~> 0.8.0`
- Removed `http_api_num_acceptors` workaround (fixed upstream in Electric via [#2863](https://github.com/electric-sql/electric/pull/2863))
- Updated `Electric.StatusMonitor.mark_connection_pool_ready/2` calls to `/3` (Electric 1.2.x API change)
- Updated storage configuration to use keyword list format (Electric 1.2.x requirement)

### Deprecated

- **Sandbox mode** (`mode: :sandbox`) is deprecated and will be removed in a future version. Electric 1.2.x introduced architectural changes that make sandbox mode incompatible with the new internal structure. Use embedded mode with a test database instead.
- **LiveView streams** (`Phoenix.Sync.LiveView.sync_stream/4` and `sync_stream_update/3`) are deprecated and will be removed in a future version. Use `Phoenix.Sync.Shape` or client-side sync with TanStack DB instead.

### Added

- Implemented `Inspector.load_supported_features/1` callback (new in Electric 1.2.x)
- Implemented `PublicationManager.wait_for_restore/1` callback (new in Electric 1.2.x)
- Updated storage configuration tests to handle both keyword list and map formats for better forward compatibility

### Migration Guide

If upgrading from Phoenix.Sync 0.6.1 or earlier:

1. **Electric 1.2.x Required**: This version requires Electric 1.2.4 or later. Electric 1.1.x is no longer supported.

2. **Deprecated Features**:
   - **Sandbox mode**: No longer compatible with Electric 1.2.x. Migrate to embedded mode with test database.
   - **LiveView streams**: `sync_stream/4` and `sync_stream_update/3` will be removed in a future version. Migrate to `Phoenix.Sync.Shape` or client-side sync.

3. **Deprecated Configuration Options**:
   - `experimental_live_sse` has been replaced by `live_sse` in Electric 1.2.x
   - The `ELECTRIC_EXPERIMENTAL_MAX_SHAPES` environment variable has been retired; use the `max_shapes` configuration option instead

4. **New Configuration Options** (Electric 1.2.x):
   - `live_sse`: Enable server-sent events for real-time updates (replaces `experimental_live_sse`)
   - `replication_idle_timeout`: Automatically close database connections during idle replication streams (useful for scale-to-zero deployments)

## [0.6.1] - 2025-10-13

### Fixed

- Correctly send response within the request process for interruptible shapes ([#111](https://github.com/electric-sql/phoenix_sync/pull/111))
- Pin electric to only supported versions ([#111](https://github.com/electric-sql/phoenix_sync/pull/111))
- Correctly fetch query params within the Plug adapter ([#111](https://github.com/electric-sql/phoenix_sync/pull/111))

## [0.6.0] - 2025-09-15

### Added

- Add new [`phoenix_sync.install`](https://hexdocs.pm/phoenix_sync/Mix.Tasks.PhoenixSync.Install.html) task to simplify installation and configuration via [igniter](https://hexdocs.pm/igniter/readme.html) [#93](https://github.com/electric-sql/phoenix_sync/pull/93)
- Allow for transforming the sync stream returned by `Phoenix.Sync.Router.sync/3` using a `transform` function [#99](https://github.com/electric-sql/phoenix_sync/pull/99)
- Add new [`phx.sync.tanstack_db.setup`](https://hexdocs.pm/phoenix_sync/Mix.Tasks.Phx.Sync.TanstackDb.Setup.html) task to convert a new Phoenix app to use Tanstack DB and Vite [igniter](https://hexdocs.pm/igniter/readme.html) [#102](https://github.com/electric-sql/phoenix_sync/pull/102)

### Changed

- Bump `electric` to version `1.1.9` [#103](https://github.com/electric-sql/phoenix_sync/pull/103)

### Fixed

- Remove `Ecto` requirement from `Phoenix.Sync.LiveView.sync_stream/4` by allowing keyword-based shape definitions [#95](https://github.com/electric-sql/phoenix_sync/pull/95)
- Ensure Electric stack is ready before calling the embedded API [#104](https://github.com/electric-sql/phoenix_sync/pull/104)
- Forward HTTP request headers onto sync backend [#107](https://github.com/electric-sql/phoenix_sync/pull/107)

## [0.5.1] - 2025-08-18

### Changed

- Update `electric` to `~> 1.1` including new [faster storage engine](https://electric-sql.com/blog/2025/08/13/electricsql-v1.1-released).
- Update `electric_client` to `~> 0.7.0` which includes `txid` headers in sync messages.

### Fixed

- Use 32-bit txid to ensure consistency with Electric sync messages ([#71](https://github.com/electric-sql/phoenix_sync/pull/71))
- Only enable sandbox if both `Ecto.SQL` and `Electric` are installed ([#86](https://github.com/electric-sql/phoenix_sync/pull/86))
- Fix liveview startup [#87](https://github.com/electric-sql/phoenix_sync/issues/87) ([#88](https://github.com/electric-sql/phoenix_sync/pull/88))
- Fix occasional compilation error when `Electric` installed ([#89](https://github.com/electric-sql/phoenix_sync/pull/89))
- Include `jason` dependency if Elixir doesn't have built in JSON support ([#90](https://github.com/electric-sql/phoenix_sync/pull/90))

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
