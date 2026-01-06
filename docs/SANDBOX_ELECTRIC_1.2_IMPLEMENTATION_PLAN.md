# Sandbox Implementation Plan for Electric 1.2.x

## Overview

This document outlines the plan to ensure Phoenix.Sync.Sandbox is compatible with Electric 1.2.x. The sandbox provides test isolation by creating per-test Electric stacks with in-memory storage.

## Current Electric APIs Used by Sandbox

### Core Modules

| Module | Functions Used | File | Risk Level |
|--------|---------------|------|------------|
| `Electric.Application` | `api/1` | sandbox.ex:265 | Medium |
| `Electric.StatusMonitor` | `mark_pg_lock_acquired/2`, `mark_replication_client_ready/2`, `mark_connection_pool_ready/2` | sandbox.ex:260-262 | Low |
| `Electric.ShapeCache` | Child spec | stack.ex:113 | Medium |
| `Electric.ShapeCache.InMemoryStorage` | Storage module | stack.ex:85 | Low |
| `Electric.ShapeCache.ShapeStatus` | `opts/1`, `shape_meta_table/1` | stack.ex:94-96 | Medium |
| `Electric.ShapeCache.ShapeStatusOwner` | Child spec | stack.ex:117 | Medium |
| `Electric.ShapeCache.Storage` | `make_new_snapshot!/2` | stack.ex:67 | Medium |
| `Electric.Shapes.Monitor` | Child spec | stack.ex:126 | Medium |
| `Electric.Shapes.DynamicConsumerSupervisor` | `name/1`, child spec | stack.ex:105, 120 | High |
| `Electric.Shapes.Querying` | `stream_initial_data/4` | stack.ex:52 | Medium |
| `Electric.Replication.Supervisor` | Child spec | stack.ex:135 | High |
| `Electric.Replication.ShapeLogCollector` | `name/1`, `store_transaction/2` | stack.ex:104, producer.ex:65 | Medium |
| `Electric.Replication.Changes.*` | `Transaction`, `NewRecord`, `UpdatedRecord`, `DeletedRecord`, `TruncatedRelation` | producer.ex:5-11 | Low |
| `Electric.Replication.LogOffset` | `new/2` | producer.ex:183 | Low |
| `Electric.ProcessRegistry` | Child spec | stack.ex:124 | Low |
| `Electric.PersistentKV.Memory` | `new!/0` | stack.ex:114 | Low |
| `Electric.Postgres.Lsn` | `from_integer/1` | producer.ex:84 | Low |
| `Electric.Postgres.display_settings/0` | Display settings | stack.ex:49 | Low |

## Known Electric 1.2.x Changes

### 1. Shape Consumer Architecture (High Impact)
- Electric 1.2.x redesigned from supervisor-based to single-process consumer model
- `Electric.Shapes.DynamicConsumerSupervisor` may have changed
- `Electric.Replication.Supervisor` child spec may differ

### 2. Storage API Changes (Medium Impact)
- Storage initialization format changed from keyword list to map in some contexts
- Already handled in `application_test.exs` with flexible assertions

### 3. Configuration Changes (Low Impact)
- `experimental_live_sse` → `live_sse`
- `ELECTRIC_EXPERIMENTAL_MAX_SHAPES` retired → use `max_shapes`
- New `replication_idle_timeout` option

## Implementation Steps

### Phase 1: API Verification (Research)
- [ ] Verify `Electric.Application.api/1` signature in 1.2.x
- [ ] Verify `Electric.StatusMonitor.mark_*` functions exist
- [ ] Verify `Electric.Replication.Supervisor` child spec format
- [ ] Verify `Electric.Shapes.DynamicConsumerSupervisor` API
- [ ] Verify `Electric.ShapeCache.ShapeStatus` API
- [ ] Verify `Electric.Shapes.Monitor` initialization options
- [ ] Verify `Electric.Replication.ShapeLogCollector.store_transaction/2` signature

### Phase 2: Stack Configuration Updates (stack.ex)
- [ ] Update `config/3` function if needed
- [ ] Update `init/1` supervisor children if needed
- [ ] Update `snapshot_query/7` if `Querying.stream_initial_data/4` changed
- [ ] Verify `Electric.ShapeCache.Storage.make_new_snapshot!/2` API

### Phase 3: Producer Updates (producer.ex)
- [ ] Verify `ShapeLogCollector.store_transaction/2` still works
- [ ] Verify `Electric.Replication.Changes.*` struct constructors
- [ ] Verify `UpdatedRecord.new/1` factory function
- [ ] Verify `LogOffset.new/2` signature

### Phase 4: Test Verification
- [ ] Run `mix test test/phoenix/sync/sandbox_test.exs`
- [ ] Run `mix test test/phoenix/sync/sandbox/sandbox_repo_test.exs`
- [ ] Run `mix test test/phoenix/sync/sandbox/sandbox_shared_test.exs`
- [ ] Run `mix test test/phoenix/sync/sandbox/sandbox_adapter_test.exs`
- [ ] Fix any failing tests

### Phase 5: Documentation
- [ ] Update CHANGELOG if additional changes needed
- [ ] Add any migration notes for sandbox users

## Potential Breaking Points

### High Risk
1. **`Electric.Replication.Supervisor` child spec** - The shape consumer architecture was redesigned in 1.2.x
2. **`Electric.Shapes.DynamicConsumerSupervisor`** - May have different initialization

### Medium Risk
3. **`Electric.Application.api/1`** - May return different struct format
4. **`Electric.ShapeCache` initialization** - Config format may differ
5. **`Electric.Shapes.Querying.stream_initial_data/4`** - Internal API, may change

### Low Risk
6. **`Electric.StatusMonitor.mark_*`** - Lifecycle API likely stable
7. **`Electric.Replication.Changes.*`** - Data structures unlikely to change
8. **`Electric.Postgres.Lsn`** - Utility functions unlikely to change

## Fallback Strategy

If Electric 1.2.x has significant breaking changes:

1. **Add version detection** - Use `Code.ensure_loaded?` and `function_exported?` to detect API availability
2. **Create adapter layer** - Abstract Electric internals behind Phoenix.Sync interfaces
3. **Request Electric support** - Electric explicitly supports Phoenix.Sync (see `api_plug_opts/1` docs)

## Testing Strategy

```bash
# Run all sandbox tests
mix test --only sandbox

# Run specific test files
mix test test/phoenix/sync/sandbox_test.exs
mix test test/phoenix/sync/sandbox/sandbox_repo_test.exs

# Run with verbose output
mix test --only sandbox --trace
```

## Success Criteria

- [ ] All sandbox tests pass with Electric 1.2.4
- [ ] No deprecation warnings from Electric APIs
- [ ] Sandbox start/stop lifecycle works correctly
- [ ] Change propagation (insert/update/delete) works
- [ ] LiveView integration works
- [ ] Router/Controller integration works
