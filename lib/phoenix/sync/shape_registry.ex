defmodule Phoenix.Sync.ShapeRegistry do
  @moduledoc """
  Track active shape subscriptions. This allows them to be interrupted.
  """
  use GenServer

  alias Electric.Client.ShapeDefinition

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register_shape(key, %ShapeDefinition{} = shape) when is_binary(key) do
    GenServer.call(__MODULE__, {:register, key, shape})
  end

  def register_shape(key) when is_binary(key) do
    GenServer.call(__MODULE__, {:unregister, key})
  end

  def interrupt_matching(matcher) when is_function(matcher, 1) do
    GenServer.call(__MODULE__, {:interrupt_matching, matcher})
  end

  def interrupt_matching(module) when is_atom(module) do
    if function_exported?(module, :__schema__, 1) do
      table = module.__schema__(:source)
      namespace = module.__schema__(:prefix)

      interrupt_matching(table, namespace)
    else
      {:error, {:not_an_ecto_schema, module}}
    end
  end

  def interrupt_matching(table, nil) when is_binary(table) do
    interrupt_matching(fn %ShapeDefinition{table: shape_table} ->
      shape_table === table
    end)
  end

  def interrupt_matching(table, namespace) when is_binary(table) and is_binary(namespace) do
    interrupt_matching(fn %ShapeDefinition{table: shape_table, namespace: shape_namespace} ->
      table === shape_table and namespace === shape_namespace
    end)
  end

  # Server implementation

  def init(_opts) do
    {:ok, %{subscriptions: %{}, monitors: %{}}}
  end

  def handle_call({:register, key, shape}, _from, state) do
    # Monitor the request process
    monitor_ref = Process.monitor(shape.request_pid)

    state = %{
      state
      | subscriptions: Map.put(state.subscriptions, key, shape),
        monitors: Map.put(state.monitors, monitor_ref, key)
    }

    {:reply, :ok, state}
  end

  def handle_call({:interrupt_matching, matcher}, _from, state) do
    interrupted_count =
      state.subscriptions
      |> Enum.filter(fn {_id, shape} -> matcher.(shape) end)
      |> Enum.reduce(0, fn {key, shape}, acc ->
        send(shape.request_pid, {:interrupt_shape, key, :server_interrupt})

        acc + 1
      end)

    {:reply, {:ok, interrupted_count}, state}
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {key, monitors} when is_binary(key) ->
        subscriptions = Map.delete(state.subscriptions, key)

        {:noreply, %{state | subscriptions: subscriptions, monitors: monitors}}

      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}
    end
  end
end
