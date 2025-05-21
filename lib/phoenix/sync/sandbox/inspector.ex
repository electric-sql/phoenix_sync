defmodule Phoenix.Sync.Sandbox.Inspector do
  @moduledoc false

  use GenServer

  @behaviour Electric.Postgres.Inspector

  @impl Electric.Postgres.Inspector

  def load_relation_oid(relation, stack_id) do
    GenServer.call(name(stack_id), {:load_relation_oid, relation})
  end

  @impl Electric.Postgres.Inspector
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
    Phoenix.Sync.Sandbox.name({__MODULE__, stack_id})
  end

  def current_xmax(stack_id) do
    GenServer.call(name(stack_id), :current_xmax)
  end

  @impl GenServer
  def init(args) do
    {:ok, stack_id} = Keyword.fetch(args, :stack_id)
    {:ok, repo} = Keyword.fetch(args, :repo)

    {:ok, %{repo: repo, stack_id: stack_id, relations: %{}}}
  end

  @impl GenServer
  def handle_call({:load_relation_oid, relation}, _from, state) do
    {
      :reply,
      Electric.Postgres.Inspector.DirectInspector.load_relation_oid(relation, pool(state)),
      state
    }
  end

  def handle_call({:load_relation_info, relation}, _from, state) do
    {
      :reply,
      Electric.Postgres.Inspector.DirectInspector.load_relation_info(relation, pool(state)),
      state
    }
  end

  def handle_call({:load_column_info, relation}, _from, state) do
    {
      :reply,
      Electric.Postgres.Inspector.DirectInspector.load_column_info(relation, pool(state)),
      state
    }
  end

  def handle_call(:current_xmax, _from, state) do
    reply =
      with {:ok, %{rows: [[xmax]]}} <-
             Postgrex.query(pool(state), "SELECT pg_snapshot_xmax(pg_current_snapshot())", []) do
        {:ok, xmax}
      end

    {:reply, reply, state}
  end

  defp pool(state) do
    %{pid: pool} = Ecto.Adapter.lookup_meta(state.repo.get_dynamic_repo())
    pool
  end
end
