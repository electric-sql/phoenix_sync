if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.Storage do
    @moduledoc false
    # Custom storage wrapper for sandbox mode that skips DB snapshot creation.
    #
    # In sandbox mode, the Ecto connection is already in a transaction (from SQL.Sandbox),
    # so we can't use Postgrex.transaction for snapshots. This wrapper returns true for
    # snapshot_started?/1 so the Snapshotter skips the DB query.
    #
    # The test data will flow through the Producer which intercepts Repo writes.

    alias Electric.ShapeCache.InMemoryStorage, as: MS

    @behaviour Electric.ShapeCache.Storage

    # Delegate shared_opts to InMemoryStorage with our module name
    @impl Electric.ShapeCache.Storage
    def shared_opts(opts) do
      stack_id = Access.fetch!(opts, :stack_id)
      table_base_name = Access.get(opts, :table_base_name, __MODULE__)

      %{
        table_base_name: table_base_name,
        stack_id: stack_id
      }
    end

    # for_shape creates an InMemoryStorage struct for the shape
    @impl Electric.ShapeCache.Storage
    def for_shape(shape_handle, %{shape_handle: shape_handle} = opts) do
      opts
    end

    def for_shape(shape_handle, %{table_base_name: table_base_name, stack_id: stack_id}) do
      snapshot_table_name = :"#{table_base_name}.Snapshot_#{shape_handle}"
      log_table_name = :"#{table_base_name}.Log_#{shape_handle}"
      chunk_checkpoint_table_name = :"#{table_base_name}.ChunkCheckpoint_#{shape_handle}"

      %MS{
        table_base_name: table_base_name,
        shape_handle: shape_handle,
        snapshot_table: snapshot_table_name,
        log_table: log_table_name,
        chunk_checkpoint_table: chunk_checkpoint_table_name,
        stack_id: stack_id
      }
    end

    # CRITICAL: Always return true to skip DB snapshot creation in sandbox mode
    @impl Electric.ShapeCache.Storage
    def snapshot_started?(%MS{} = _opts), do: true

    # Delegate all other functions to InMemoryStorage
    @impl Electric.ShapeCache.Storage
    defdelegate stack_start_link(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate start_link(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate init_writer!(opts, shape_definition, storage_recovery_state), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate get_all_stored_shape_handles(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate get_all_stored_shapes(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate set_pg_snapshot(pg_snapshot, opts), to: MS

    # Override get_current_position to provide a fake pg_snapshot for sandbox mode
    # This is needed because when snapshot_started? returns true, the Consumer
    # expects a pg_snapshot to be available from storage
    @impl Electric.ShapeCache.Storage
    def get_current_position(%MS{} = opts) do
      case MS.get_current_position(opts) do
        {:ok, offset, nil} ->
          # Provide a fake pg_snapshot for sandbox mode
          # xmin=1000, xmax=1100 are safe values that won't filter any transactions
          fake_pg_snapshot = %{xmin: 1000, xmax: 1100, xip_list: [], filter_txns?: false}
          {:ok, offset, fake_pg_snapshot}

        result ->
          result
      end
    end

    @impl Electric.ShapeCache.Storage
    defdelegate get_log_stream(offset, max_offset, opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate get_chunk_end_log_offset(offset, opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate metadata_backup_dir(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate get_total_disk_usage(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate make_new_snapshot!(data_stream, opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate mark_snapshot_as_started(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate append_to_log!(log_items, opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate cleanup!(opts), to: MS

    @impl Electric.ShapeCache.Storage
    def cleanup!(opts, shape_handle), do: MS.cleanup!(opts, shape_handle)

    @impl Electric.ShapeCache.Storage
    defdelegate cleanup_all!(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate compact(opts, keep_complete_chunks), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate terminate(opts), to: MS

    @impl Electric.ShapeCache.Storage
    defdelegate hibernate(opts), to: MS
  end
end
