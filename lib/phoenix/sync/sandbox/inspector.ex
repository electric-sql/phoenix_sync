if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
  defmodule Phoenix.Sync.Sandbox.Inspector do
    @moduledoc false

    use GenServer

    @behaviour Electric.Postgres.Inspector

    @impl Electric.Postgres.Inspector

    def load_relation_oid(relation, stack_id) do
      with {:ok, pid} <- validate_stack_alive(stack_id) do
        GenServer.call(pid, {:load_relation_oid, relation})
      end
    end

    @impl Electric.Postgres.Inspector
    def load_relation_info(relation, stack_id) do
      with {:ok, pid} <- validate_stack_alive(stack_id) do
        GenServer.call(pid, {:load_relation_info, relation})
      end
    end

    @impl Electric.Postgres.Inspector
    def load_column_info(relation, stack_id) do
      with {:ok, pid} <- validate_stack_alive(stack_id) do
        GenServer.call(pid, {:load_column_info, relation})
      end
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

    defp validate_stack_alive(stack_id) do
      case GenServer.whereis(name(stack_id)) do
        nil ->
          {:error, "stack #{inspect(stack_id)} is not available"}

        pid ->
          {:ok, pid}
      end
    end

    @impl GenServer
    def init(args) do
      {:ok, stack_id} = Keyword.fetch(args, :stack_id)
      {:ok, repo} = Keyword.fetch(args, :repo)

      {:ok, %{repo: repo, stack_id: stack_id, relations: %{}, columns: %{}, oids: %{}}}
    end

    @impl GenServer
    def handle_call({:load_relation_oid, relation}, _from, state) do
      {result, state} =
        fetch_lazy(state, :oids, relation, fn ->
          Electric.Postgres.Inspector.DirectInspector.load_relation_oid(relation, pool(state))
        end)

      {:reply, result, state}
    end

    def handle_call({:load_relation_info, relation}, _from, state) do
      {result, state} =
        fetch_lazy(state, :relations, relation, fn ->
          Electric.Postgres.Inspector.DirectInspector.load_relation_info(relation, pool(state))
        end)

      {:reply, result, state}
    end

    def handle_call({:load_column_info, relation}, _from, state) do
      {result, state} =
        fetch_lazy(state, :columns, relation, fn ->
          Electric.Postgres.Inspector.DirectInspector.load_column_info(relation, pool(state))
        end)

      {:reply, result, state}
    end

    defp pool(state) do
      %{pid: pool} = Ecto.Adapter.lookup_meta(state.repo.get_dynamic_repo())
      pool
    end

    defp fetch_lazy(state, cache, key, fun) do
      case Map.fetch(Map.fetch!(state, cache), key) do
        {:ok, value} ->
          {{:ok, value}, state}

        :error ->
          case handling_exits(fun, state) do
            {:ok, value} ->
              {{:ok, value}, Map.update!(state, cache, &Map.put(&1, key, value))}

            {:error, _reason} = error ->
              {error, state}
          end
      end
    end

    defp handling_exits(fun, %{stack_id: stack_id}) do
      try do
        fun.()
      catch
        :exit, _reason -> {:error, "Stack #{inspect(stack_id)} down"}
      end
    end
  end
end
