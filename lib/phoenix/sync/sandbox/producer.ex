if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.Producer do
    @moduledoc false

    alias Electric.Replication.Changes.{
      Transaction,
      NewRecord,
      UpdatedRecord,
      DeletedRecord,
      TruncatedRelation
    }

    alias Electric.Replication.LogOffset
    alias Electric.Replication.ShapeLogCollector

    @json if(Code.ensure_loaded?(JSON), do: JSON, else: Jason)

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

    def truncate(stack_id, relation) do
      GenServer.cast(name(stack_id), {:truncate, relation})
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

    def handle_cast({:truncate, relation}, state) do
      changes = [%TruncatedRelation{relation: relation, log_offset: log_offset(state.txid, 0)}]

      :ok =
        state.txid
        |> transaction(changes)
        |> ShapeLogCollector.store_transaction(ShapeLogCollector.name(state.stack_id))

      {:noreply, %{state | txid: state.txid + 100}}
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
          record: record(values, schema_meta),
          log_offset: log_offset(txid, i)
        },
        lsn + 100
      }
    end

    defp msg_from_change({{:update, schema_meta, old, new}, i}, lsn, txid) do
      {
        UpdatedRecord.new(
          relation: relation(schema_meta),
          old_record: record(old, schema_meta),
          record: record(new, schema_meta),
          log_offset: log_offset(txid, i)
        ),
        lsn + 100
      }
    end

    defp msg_from_change({{:delete, schema_meta, old}, i}, lsn, txid) do
      {
        %DeletedRecord{
          relation: relation(schema_meta),
          old_record: record(old, schema_meta),
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

    defp record(values, %{schema: schema}) do
      Map.new(values, &load_value(&1, schema))
    end

    defp load_value({field, raw_value}, schema) do
      type = schema.__schema__(:type, field)

      {:ok, value} =
        Ecto.Type.adapter_load(
          Ecto.Adapters.Postgres,
          type,
          raw_value
        )

      # Converts to lower level postgrex type which depends on the type's
      # `type/0` value. Postgrex does the actual serialization of maps in real
      # usage so this converts embed structs to plain maps.
      #
      # Postgrex also encodes date & time types, lists and decimals itself (so
      # ecto just leaves these as-is) so the `dump/1` function needs to do the
      # work normally done by postgrex
      {:ok, value} = Ecto.Type.dump(type, value)

      {to_string(field), dump(value, type)}
    end

    defp dump(%Decimal{} = decimal, _type) do
      Decimal.to_string(decimal)
    end

    defp dump(%type{} = datetime, _type)
         when type in [NaiveDateTime, DateTime, Time, Date] do
      type.to_iso8601(datetime)
    end

    defp dump(map, _type) when is_map(map), do: @json.encode!(map)

    defp dump(list, type) when is_list(list) do
      if encode_list_json?(type) do
        @json.encode!(list)
      else
        encode_array(list, type)
      end
    end

    defp dump(nil, _type), do: nil
    defp dump(value, _type), do: to_string(value)

    defp log_offset(txid, index) do
      LogOffset.new(txid, index)
    end

    defp encode_list_json?(type) do
      case type do
        {:array, _inner_type} ->
          false

        {t, _} when t in [:map, :json, :jsonb] ->
          true

        {:parameterized, {module, params}} ->
          encode_list_json?(module.type(params))

        t ->
          if function_exported?(t, :type, 0) do
            encode_list_json?(t.type())
          else
            false
          end
      end
    end

    @doc ~S"""
    ## Examples

        iex> encode_array([1, 2, 3], {:array, :integer})
        "{1,2,3}"

        iex> encode_array(["a", "b", "c"], {:array, :string})
        ~s|{"a","b","c"}|

        iex> encode_array(["\"a\"", "b", "c"], {:array, :string})
        ~S|{"\"a\"","b","c"}|

        iex> encode_array([], {:array, :string})
        "{}"

        iex> encode_array([1, nil, 3], {:array, :integer})
        "{1,NULL,3}"

        iex> encode_array([[1, [2]], [3, 4]], {:array, :integer})
        "{{1,{2}},{3,4}}"

        iex> encode_array([%{value: 1}, %{value: 2}], {:array, :jsonb})
        ~S|{"{\"value\":1}","{\"value\":2}"}|
    """
    def encode_array(array, type) when is_list(array) do
      encode_array_inner(array, type) |> IO.iodata_to_binary()
    end

    defp encode_array_inner(array, type) do
      [?{, Enum.map_intersperse(array, ",", &encode_value(&1, type)), ?}]
    end

    defp encode_value(list, type) when is_list(list) do
      encode_array_inner(list, type)
    end

    defp encode_value(nil, _type) do
      "NULL"
    end

    defp encode_value(value, _type) when is_binary(value) do
      [?", String.replace(value, "\"", "\\\""), ?"]
    end

    defp encode_value(int, _type) when is_integer(int) do
      to_string(int)
    end

    defp encode_value(value, {:array, value_type} = type) do
      value |> dump(value_type) |> encode_value(type)
    end
  end
end
