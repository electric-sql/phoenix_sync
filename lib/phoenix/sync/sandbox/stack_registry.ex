if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
  defmodule Phoenix.Sync.Sandbox.StackRegistry do
    @moduledoc false

    use GenServer

    def start_link(_) do
      GenServer.start_link(__MODULE__, [], name: __MODULE__)
    end

    def register(pid, stack_id) do
      GenServer.call(__MODULE__, {:register, pid, stack_id})
    end

    def lookup(pid) do
      GenServer.call(__MODULE__, {:lookup, pid})
    end

    def configure(stack_id, api, client) do
      GenServer.call(__MODULE__, {:configure, self(), stack_id, api, client})
    end

    # api should be removed when test pid exits
    # registered pid should be removed when it exits

    def get_api(stack_id) do
      GenServer.call(__MODULE__, {:get, :api, stack_id})
    end

    def get_client(stack_id) do
      GenServer.call(__MODULE__, {:get, :client, stack_id})
    end

    def shared_mode(owner, stack_id) do
      GenServer.call(__MODULE__, {:shared_mode, owner, stack_id})
    end

    ## callbacks

    def init(_) do
      {:ok, %{stack_pids: %{}, stacks: %{}, shared: nil}}
    end

    def handle_call({:register, pid, stack_id}, _from, state) do
      _ref = Process.monitor(pid, tag: {:down, :register})

      state = Map.update!(state, :stack_pids, &Map.put(&1, pid, stack_id))

      {:reply, :ok, state}
    end

    def handle_call({:lookup, _pid}, _from, %{shared: {_owner, stack_id}} = state) do
      {:reply, stack_id, state}
    end

    def handle_call({:lookup, pid}, _from, state) do
      {:reply, Map.get(state.stack_pids, pid), state}
    end

    def handle_call({:configure, pid, stack_id, api, client}, _from, state) do
      _ref = Process.monitor(pid, tag: {:down, :stack, stack_id})

      state = Map.update!(state, :stacks, &Map.put(&1, stack_id, %{api: api, client: client}))

      {:reply, :ok, state}
    end

    def handle_call({:get, key, stack_id}, _from, state) when key in [:api, :client] do
      {:reply, get_config(state, stack_id, key), state}
    end

    # shared mode only works for non-async tests, which means that only
    # one test is running at any time, so we can set the shared pid globally
    def handle_call({:shared_mode, owner, stack_id}, _from, %{shared: nil} = state)
        when is_pid(owner) do
      _ref = Process.monitor(owner, tag: {:down, :shared, stack_id})
      {:reply, :ok, Map.put(state, :shared, {owner, stack_id})}
    end

    def handle_call({:shared_mode, owner, stack_id}, _from, %{shared: {owner, stack_id}} = state) do
      {:reply, :ok, state}
    end

    def handle_call(
          {:shared_mode, _owner, _stack_id},
          _from,
          %{shared: {owner, stack_id}} = state
        ) do
      {:reply,
       {:error,
        "Shared mode already registered to pid #{inspect(owner)} for stack #{inspect(stack_id)}"},
       state}
    end

    def handle_info({{:down, :stack, stack_id}, _ref, :process, _pid, _reason}, state) do
      state = Map.update!(state, :stacks, &Map.delete(&1, stack_id))

      {:noreply, state}
    end

    def handle_info(
          {{:down, :shared, stack_id}, _ref, :process, pid, _reason},
          %{shared: {pid, stack_id}} = state
        ) do
      {:noreply, Map.put(state, :shared, nil)}
    end

    def handle_info({{:down, :register}, _ref, :process, pid, _reason}, state) do
      state = Map.update!(state, :stack_pids, &Map.delete(&1, pid))

      {:noreply, state}
    end

    defp get_config(state, stack_id, key) do
      with {:ok, config} <- Map.fetch(state.stacks, stack_id) do
        Map.fetch(config, key)
      end
    end
  end
end
