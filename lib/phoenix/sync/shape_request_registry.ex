defmodule Phoenix.Sync.ShapeRequestRegistry do
  @moduledoc """
  Track active shape subscriptions so they can be interrupted.

  Note that the implementation of register and unregister depend on the call
  coming from the same process that's requesting the shape. We then register
  the pid along with the shape definition and then when interrupting, we
  brutally kill that pid if the shape definition matches.
  """
  use GenServer

  alias Electric.Client.ShapeDefinition
  alias Phoenix.Sync.PredefinedShape

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register_shape(key, %ShapeDefinition{} = shape_definition) when is_binary(key) do
    register_shape(key, PredefinedShape.from_shape_definition(shape_definition))
  end

  def register_shape(key, %PredefinedShape{query: _querable, relation: nil} = shape)
      when is_binary(key) do
    register_shape(key, PredefinedShape.from_queryable!(shape))
  end

  def register_shape(key, %PredefinedShape{relation: {_namespace, _table}} = shape)
      when is_binary(key) do
    GenServer.call(__MODULE__, {:register, key, shape})
  end

  def unregister_shape(key) when is_binary(key) do
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

  def interrupt_matching(table) when is_binary(table) do
    interrupt_matching(fn %PredefinedShape{relation: {_, shape_table}} ->
      shape_table == table
    end)
  end

  def interrupt_matching(table, nil) when is_binary(table) do
    interrupt_matching(table)
  end

  def interrupt_matching(table, namespace) when is_binary(table) and is_binary(namespace) do
    interrupt_matching(fn %PredefinedShape{relation: relation} ->
      relation == {namespace, table}
    end)
  end

  # Server implementation

  def init(_opts) do
    {:ok, %{subscriptions: %{}, monitors: %{}}}
  end

  def handle_call({:register, key, shape}, {request_pid, _ref}, state) do
    %{
      monitors: monitors,
      subscriptions: subscriptions
    } = state

    monitor_ref = Process.monitor(request_pid)
    monitors = Map.put(monitors, monitor_ref, key)

    shape_info = {shape, request_pid}
    subscriptions = Map.put(subscriptions, key, shape_info)

    {:reply, :ok, %{state | monitors: monitors, subscriptions: subscriptions}}
  end

  def handle_call({:unregister, key}, _from, %{subscriptions: subscriptions} = state) do
    {:reply, :ok, %{state | subscriptions: Map.delete(subscriptions, key)}}
  end

  def handle_call({:interrupt_matching, matcher}, _from, state) do
    interrupted_count =
      state.subscriptions
      |> Enum.filter(fn {_id, {shape, _request_pid}} -> matcher.(shape) end)
      |> Enum.reduce(0, fn {key, {_shape, request_pid}}, acc ->
        send(request_pid, {:interrupt_shape, key, :server_interrupt})

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
