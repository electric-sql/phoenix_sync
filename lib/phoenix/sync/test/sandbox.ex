defmodule Phoenix.Sync.Test do
  def init_test_session(conn, repo, session) do
    {:ok, sandbox} = Phoenix.Sync.Test.Sandbox.sandbox_conn(repo)
    dbg(sandbox)

    client = {Client.Sandbox, conn: sandbox}

    # conn = put_in(conn.private)

    conn
    |> Plug.Test.init_test_session(session)
    |> Plug.Conn.put_private(:electric_client, client)
  end
end

defmodule Phoenix.Sync.Test.Sandbox do
  use Supervisor

  @registry __MODULE__.Registry

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def checkout(repo) when is_atom(repo) do
    {:ok, conn} = sandbox_conn(repo)

    dbg(conn: conn)

    # DBConnection.Ownership.Manager.proxy_for([self()]) |> dbg

    case GenServer.whereis(name({Producer, conn})) do
      nil ->
        # stack_id = "#{inspect(__MODULE__.Stack)}#{System.monotonic_time()}"
        stack_id = "#{inspect(__MODULE__.Stack)}" |> dbg

        # DynamicSupervisor.start_child(
        #   @producer_supervisor,
        #   {__MODULE__.Stack, repo: repo, stack_id: stack_id, conn: conn}
        # )
        #
        {:ok, _pid} = __MODULE__.Stack.start_link(conn, stack_id, repo)

      pid when is_pid(pid) ->
        :ok
    end
  end

  def sandbox_conn(repo) do
    repo.get_dynamic_repo()
    |> Ecto.Adapter.lookup_meta()
    |> tap(&dbg/1)
    |> Phoenix.Sync.Test.Adapter.conn()
  end

  def name(id) do
    {:via, Registry, {@registry, id}}
  end

  ## callbacks

  def init(_) do
    children = [
      {Registry, keys: :unique, name: @registry}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Phoenix.Sync.Test.Sandbox.ClientAdapter do
  @behaviour Electric.Client.Fetch.Pool

  @impl Electric.Client.Fetch.Pool
  def request(%Electric.Client{} = client, %Electric.Client.Fetch.Request{} = request, opts) do
    dbg(client: client, request: request)
    %{}
  end
end

defmodule Phoenix.Sync.Test.Sandbox.PublicationManager do
  @behaviour Electric.Replication.PublicationManager

  def start_link(_) do
    :ignore
  end

  def name(stack_id) when is_binary(stack_id) do
    Phoenix.Sync.Test.Sandbox.name({__MODULE__, stack_id})
  end

  def name(opts) when is_list(opts) do
    opts
    |> Keyword.fetch!(:stack_id)
    |> name()
  end

  def recover_shape(_shape_handle, _shape, _opts) do
    :ok
  end

  def recover_shape(_shape, _opts) do
    :ok
  end

  def add_shape(_shape_handle, _shape, _opts) do
    :ok
  end

  def add_shape(_shape, _opts) do
    :ok
  end

  def remove_shape(_shape_handle, _shape, _opts) do
    :ok
  end

  def remove_shape(_shape, _opts) do
    :ok
  end

  def refresh_publication(_opts) do
    :ok
  end
end

defmodule Phoenix.Sync.Test.Sandbox.Stack do
  use Supervisor, restart: :transient

  alias Phoenix.Sync.Test.Sandbox

  def child_spec(opts) do
    {:ok, conn} = Keyword.fetch(opts, :conn)
    {:ok, stack_id} = Keyword.fetch(opts, :stack_id)
    {:ok, repo} = Keyword.fetch(opts, :repo)

    %{
      id: {__MODULE__, {conn, stack_id}},
      start: {__MODULE__, :start_link, [conn, stack_id, repo]},
      type: :supervisor,
      restart: :transient
    }
  end

  def name(conn, stack_id) do
    Phoenix.Sync.Test.Sandbox.name({__MODULE__, {conn, stack_id}})
  end

  def start_link(conn, stack_id, repo) do
    Supervisor.start_link(__MODULE__, {conn, stack_id, repo}, name: name(conn, stack_id))
  end

  def init({conn, stack_id, repo}) do
    publication_manager_spec = {Sandbox.PublicationManager, stack_id: stack_id}
    # shape_cache_opts = [
    #   purge_all_shapes?: true,
    #   stack_id: stack_id,
    #   storage:
    #     {Electric.ShapeCache.InMemoryStorage,
    #      %{stack_id: stack_id, table_base_name: :"#{stack_id}"}},
    #   inspector: {Sandbox.Inspector, stack_id},
    #   publication_manager: publication_manager_spec,
    #   chunk_bytes_threshold: 10_485_760,
    #   log_producer:
    #     {:via, Registry,
    #      {:"Electric.ProcessRegistry:Elixir.Electric.Plug.RouterTest test /v1/shapes HEAD receives all headers",
    #       {Electric.Replication.ShapeLogCollector, nil}}},
    #   consumer_supervisor:
    #     {:via, Registry,
    #      {:"Electric.ProcessRegistry:Elixir.Electric.Plug.RouterTest test /v1/shapes HEAD receives all headers",
    #       {Electric.Shapes.DynamicConsumerSupervisor, nil}}},
    #   registry:
    #     :"Elixir.Registry.ShapeChanges:Elixir.Electric.Plug.RouterTest test /v1/shapes HEAD receives all headers",
    #   max_shapes: nil
    # ]

    # shape_cache_spec = {Electric.ShapeCache, shape_cache_opts}

    children = [
      # TODO: start an electric stack, decoupled from the db connection
      #       with in memory storage, a mock publication_manager and inspector
      # Supervisor.child_spec(
      #   {
      #     Electric.Replication.Supervisor,
      #     stack_id: stack_id,
      #     shape_cache: shape_cache_spec,
      #     publication_manager: publication_manager_spec,
      #     log_collector: shape_log_collector_spec,
      #     schema_reconciler: schema_reconciler_spec
      #   },
      #   restart: :temporary
      # ),
      {Sandbox.Producer, stack_id: stack_id, conn: conn},
      {Sandbox.Inspector, stack_id: stack_id, repo: repo}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Phoenix.Sync.Test.Sandbox.Inspector do
  use GenServer

  @behaviour Electric.Postgres.Inspector

  @impl Electric.Postgres.Inspector

  def load_relation_oid(relation, stack_id) do
    GenServer.call(name(stack_id), {:load_relation_oid, relation})
  end

  def load_relation_info(relation, stack_id) do
    GenServer.call(name(stack_id), {:load_relation_info, relation})
  end

  @impl Electric.Postgres.Inspector
  def load_column_info(relation, stack_id) do
    GenServer.call(name(stack_id), {:load_column_info, relation})
  end

  @impl Electric.Postgres.Inspector
  def clean(_, _), do: true

  @impl Electric.Postgres.Inspector
  def list_relations_with_stale_cache(_), do: {:ok, []}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args[:stack_id]))
  end

  def name(stack_id) do
    Phoenix.Sync.Test.Sandbox.name({__MODULE__, stack_id})
  end

  @impl GenServer
  def init(args) do
    {:ok, stack_id} = Keyword.fetch(args, :stack_id)
    {:ok, repo} = Keyword.fetch(args, :repo)

    repo_config = apply(repo, :config, []) |> dbg

    {:ok, conn} =
      Postgrex.start_link(
        Keyword.merge(repo_config, pool_size: 1, pool: DBConnection.ConnectionPool)
      )

    {:ok, %{conn: conn, stack_id: stack_id, relations: %{}}}
  end

  @impl GenServer
  def handle_call({:load_relation_oid, relation}, _from, state) do
    dbg(load_relation_oid: relation)

    {
      :reply,
      Electric.Postgres.Inspector.DirectInspector.load_relation_oid(relation, state.conn),
      state
    }
  end

  def handle_call({:load_relation_info, relation}, _from, state) do
    dbg(load_relation_info: relation)

    {
      :reply,
      Electric.Postgres.Inspector.DirectInspector.load_relation_info(relation, state.conn),
      state
    }
  end

  def handle_call({:load_column_info, relation}, _from, state) do
    dbg(load_column_info: relation)

    {
      :reply,
      Electric.Postgres.Inspector.DirectInspector.load_relation_info(relation, state.conn),
      state
    }
  end
end

defmodule Phoenix.Sync.Test.Sandbox.Producer do
  alias Electric.Replication.Changes.{
    Transaction,
    NewRecord,
    UpdatedRecord,
    DeletedRecord
  }

  alias Electric.Replication.LogOffset
  alias Phoenix.Sync.Test.Sandbox

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

  def init({_conn, stack_id}) do
    state = %{txid: 10000, stack_id: stack_id}
    {:ok, state}
  end

  def handle_cast({:emit_changes, changes}, %{txid: txid, stack_id: stack_id} = state) do
    {msgs, next_txid} =
      changes
      |> Enum.with_index(0)
      |> Enum.map_reduce(txid, &msg_from_change(&1, &2, txid))

    dbg(emit: transaction(txid, msgs))

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
