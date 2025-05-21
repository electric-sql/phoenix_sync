defmodule Phoenix.Sync.Test.Sandbox do
  use Supervisor

  @registry __MODULE__.Registry
  @producer_supervisor __MODULE__.ProducerSupervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def checkout(repo) when is_atom(repo) do
    {:ok, conn} = sandbox_conn(repo)

    case GenServer.whereis(name({Producer, conn})) do
      nil ->
        stack_id = "#{inspect(__MODULE__.Stack)}#{System.monotonic_time()}"

        DynamicSupervisor.start_child(
          @producer_supervisor,
          {__MODULE__.Stack, stack_id: stack_id, conn: conn}
        )
        |> dbg

      pid when is_pid(pid) ->
        :ok
    end
  end

  defp sandbox_conn(repo) do
    repo
    |> GenServer.whereis()
    |> Ecto.Adapter.lookup_meta()
    |> Phoenix.Sync.Test.Adapter.conn()
  end

  def name(id) do
    {:via, Registry, {@registry, id}}
  end

  ## callbacks

  def init(_) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, name: @producer_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Phoenix.Sync.Test.Sandbox.Stack do
  use Supervisor, restart: :transient

  def child_spec(opts) do
    {:ok, conn} = Keyword.fetch(opts, :conn)
    {:ok, stack_id} = Keyword.fetch(opts, :stack_id)

    %{
      id: {__MODULE__, {conn, stack_id}},
      start: {__MODULE__, :start_link, [conn, stack_id]},
      type: :supervisor,
      restart: :transient
    }
  end

  def name(conn, stack_id) do
    Phoenix.Sync.Test.Sandbox.name({__MODULE__, {conn, stack_id}})
  end

  def start_link(conn, stack_id) do
    Supervisor.start_link(__MODULE__, {conn, stack_id}, name: name(conn, stack_id))
  end

  def init({conn, stack_id}) do
    children = [
      {Phoenix.Sync.Test.Sandbox.Producer, stack_id: stack_id, conn: conn}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Phoenix.Sync.Test.Sandbox.Producer do
  alias Electric.Replication.Changes.{
    Transaction,
    NewRecord,
    UpdatedRecord,
    DeletedRecord
    # Relation,
    # Column
  }

  alias Electric.Replication.LogOffset

  def child_spec(opts) do
    {:ok, conn} = Keyword.fetch(opts, :conn)
    {:ok, stack_id} = Keyword.fetch(opts, :stack_id)

    %{
      id: {__MODULE__, {conn, stack_id}},
      start: {__MODULE__, :start_link, [conn, stack_id]},
      type: :worker,
      restart: :transient
    }
  end

  def emit_changes(conn, changes) do
    GenServer.cast(name(conn), {:emit_changes, changes})
  end

  def name(conn) do
    Phoenix.Sync.Test.Sandbox.name({__MODULE__, {conn}})
  end

  def start_link(conn, stack_id) do
    GenServer.start_link(__MODULE__, {conn, stack_id}, name: name(conn))
  end

  def init({conn, stack_id}) do
    state = %{txid: 10000}
    {:ok, state}
  end

  def handle_cast({:emit_changes, changes}, %{txid: txid} = state) do
    {msgs, txid} = changes |> Enum.with_index(0) |> Enum.map_reduce(txid, &msg_from_change/2)
    dbg(emit: transaction(txid, msgs))

    {:noreply, %{state | txid: txid}}
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

  defp msg_from_change({{:insert, {prefix, source}, values}, i}, txid) do
    {
      %NewRecord{
        relation: {namespace(prefix), source},
        record: record(values),
        log_offset: log_offset(txid, i)
      },
      txid + 1
    }
  end

  defp msg_from_change({{:update, {prefix, source}, old, new}, i}, txid) do
    {
      UpdatedRecord.new(
        relation: {namespace(prefix), source},
        old_record: record(old),
        record: record(new),
        log_offset: log_offset(txid, i)
      ),
      txid + 1
    }
  end

  defp msg_from_change({{:delete, {prefix, source}, old}, i}, txid) do
    {
      %DeletedRecord{
        relation: {namespace(prefix), source},
        old_record: record(old),
        log_offset: log_offset(txid, i)
      },
      txid + 1
    }
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
