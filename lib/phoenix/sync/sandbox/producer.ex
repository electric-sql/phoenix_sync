if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
  defmodule Phoenix.Sync.Sandbox.Producer do
    @moduledoc false

    alias Electric.Replication.Changes.{
      Transaction,
      NewRecord,
      UpdatedRecord,
      DeletedRecord
    }

    alias Electric.Replication.LogOffset
    alias Electric.Replication.ShapeLogCollector

    def child_spec(opts) do
      {:ok, stack_id} = Keyword.fetch(opts, :stack_id)

      %{
        id: {__MODULE__, stack_id},
        start: {__MODULE__, :start_link, [stack_id]},
        type: :worker,
        restart: :transient
      }
    end

    def emit_changes(stack_id \\ Phoenix.Sync.Sandbox.stack_id(), changes)

    def emit_changes(nil, _changes) do
      raise RuntimeError, "Process #{inspect(self())} is not registered to a sandbox"
    end

    def emit_changes(stack_id, changes) when is_binary(stack_id) do
      GenServer.cast(name(stack_id), {:emit_changes, changes})
    end

    def sync(stack_id) do
      GenServer.call(name(stack_id), :sync)
    end

    def name(stack_id) do
      Phoenix.Sync.Sandbox.name({__MODULE__, stack_id})
    end

    def start_link(stack_id) do
      GenServer.start_link(__MODULE__, stack_id, name: name(stack_id))
    end

    def init(stack_id) do
      state = %{txid: 10000, stack_id: stack_id}
      {:ok, state}
    end

    # get the current xmax from pg so that our change events are properly ordered
    # after the initial sync
    # has to come after start up and after we've allowed the inspector access to the
    # sandboxed connection
    def handle_call(:sync, _from, state) do
      with {:ok, xmax} <- Phoenix.Sync.Sandbox.Inspector.current_xmax(state.stack_id) do
        {:reply, :ok, Map.put(state, :txid, xmax + 1000)}
      else
        error -> {:reply, error, state}
      end
    end

    def handle_cast({:emit_changes, changes}, %{txid: txid, stack_id: stack_id} = state) do
      {msgs, next_txid} =
        changes
        |> Enum.with_index(0)
        |> Enum.map_reduce(txid, &msg_from_change(&1, &2, txid))

      :ok =
        txid
        |> transaction(msgs)
        |> ShapeLogCollector.store_transaction(ShapeLogCollector.name(stack_id))

      {:noreply, %{state | txid: next_txid}}
    end

    defp transaction(txid, changes) do
      %Transaction{
        xid: txid,
        lsn: Electric.Postgres.Lsn.from_integer(txid),
        last_log_offset: Enum.at(changes, -1) |> Map.fetch!(:log_offset),
        changes: changes,
        num_changes: length(changes),
        commit_timestamp: DateTime.utc_now(),
        affected_relations: Enum.into(changes, MapSet.new(), & &1.relation)
      }
    end

    defp msg_from_change({{:insert, schema_meta, values}, i}, lsn, txid) do
      {
        %NewRecord{
          relation: relation(schema_meta),
          record: record(values),
          log_offset: log_offset(txid, i)
        },
        lsn + 100
      }
    end

    defp msg_from_change({{:update, schema_meta, old, new}, i}, lsn, txid) do
      {
        UpdatedRecord.new(
          relation: relation(schema_meta),
          old_record: record(old),
          record: record(new),
          log_offset: log_offset(txid, i)
        ),
        lsn + 100
      }
    end

    defp msg_from_change({{:delete, schema_meta, old}, i}, lsn, txid) do
      {
        %DeletedRecord{
          relation: relation(schema_meta),
          old_record: record(old),
          log_offset: log_offset(txid, i)
        },
        lsn + 100
      }
    end

    defp relation(%{source: source, prefix: prefix}) do
      {namespace(prefix), source}
    end

    defp namespace(nil), do: "public"
    defp namespace(ns) when is_binary(ns), do: ns

    defp record(values) do
      # FIXME: should we use the schema to cast these values?
      Map.new(values, fn {k, v} -> {to_string(k), to_string(v)} end)
    end

    defp log_offset(txid, index) do
      LogOffset.new(txid, index)
    end
  end
end
