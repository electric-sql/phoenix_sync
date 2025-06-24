defmodule Phoenix.Sync.ShapeRequestRegistry do
  @moduledoc false

  use GenServer

  alias Phoenix.Sync.PredefinedShape

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register_shape(%PredefinedShape{} = shape) do
    GenServer.call(__MODULE__, {:register_shape, shape})
  end

  def unregister_shape(key) do
    GenServer.call(__MODULE__, {:unregister_shape, key})
  end

  def interrupt_matching(shape, shape_opts \\ [])

  def interrupt_matching(match_spec, _opts) when is_list(match_spec) do
    match_spec = Keyword.replace_lazy(match_spec, :params, &normalize_params_form/1)

    do_interrupt_matching(fn %PredefinedShape{} = shape ->
      params = PredefinedShape.to_shape_params(shape)

      Enum.all?(match_spec, fn {k, v} -> Keyword.get(params, k) == v end)
    end)
  end

  def interrupt_matching(matcher, _opts) when is_function(matcher, 1) do
    GenServer.call(__MODULE__, {:interrupt_matching, matcher})
  end

  def interrupt_matching(queryable, shape_opts)
      when is_atom(queryable) or is_struct(queryable, Ecto.Query) do
    match_params =
      queryable |> PredefinedShape.new!(shape_opts) |> PredefinedShape.to_shape_params()

    do_interrupt_matching(fn %PredefinedShape{} = shape ->
      match_params == PredefinedShape.to_shape_params(shape)
    end)
  rescue
    e -> {:error, e}
  end

  def interrupt_matching(table, opts) when is_binary(table) do
    interrupt_matching(Keyword.put(opts, :table, table))
  end

  defp do_interrupt_matching(fun) do
    GenServer.call(__MODULE__, {:interrupt_matching, fun})
  end

  defp normalize_params_form(params) when is_list(params) do
    params
    |> Enum.with_index(fn elem, index -> {to_string(index + 1), to_string(elem)} end)
    |> Map.new()
  end

  defp normalize_params_form(params) when is_map(params) do
    Map.new(params, fn {k, v} -> {to_string(k), to_string(v)} end)
  end

  def registered_requests do
    GenServer.call(__MODULE__, :registered_requests)
  end

  # Server implementation

  @impl GenServer
  def init(_opts) do
    {:ok, %{subscriptions: %{}, monitors: %{}}}
  end

  @impl GenServer
  def handle_call({:register_shape, shape}, {request_pid, _ref}, state) do
    %{
      monitors: monitors,
      subscriptions: subscriptions
    } = state

    key = make_ref()

    monitor_ref = Process.monitor(request_pid)
    monitors = Map.put(monitors, monitor_ref, key)

    shape_info = {shape, request_pid}
    subscriptions = Map.put(subscriptions, key, shape_info)

    {:reply, {:ok, key}, %{state | monitors: monitors, subscriptions: subscriptions}}
  end

  def handle_call({:unregister_shape, key}, _from, %{subscriptions: subscriptions} = state) do
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

  def handle_call(:registered_requests, _from, %{subscriptions: subscriptions} = state) do
    {:reply, Map.to_list(subscriptions), state}
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {key, monitors} ->
        subscriptions = Map.delete(state.subscriptions, key)

        {:noreply, %{state | subscriptions: subscriptions, monitors: monitors}}
    end
  end
end
