defmodule Phoenix.Sync.Shape.Registry do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register(pid \\ self()) do
    GenServer.call(__MODULE__, {:register, pid})
  end

  def whereis(pid) do
    case :ets.lookup(__MODULE__, GenServer.whereis(pid)) do
      [{_pid, shape_id}] -> {:ok, shape_id}
      [] -> :error
    end
  end

  def init(_opts) do
    table = :ets.new(__MODULE__, [:named_table, :protected, read_concurrency: true])
    {:ok, %{table: table, id: 0}}
  end

  def handle_call({:register, pid}, _from, state) do
    {shape_id, new_state} = shape_id(state)
    :ets.insert(state.table, {pid, shape_id})
    {:reply, {:ok, shape_id}, new_state}
  end

  defp shape_id(state) do
    {:"shape-data-#{state.id}", Map.update!(state, :id, &(&1 + 1))}
  end
end
