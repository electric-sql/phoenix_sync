defmodule PhoenixSyncExample.Sync do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def subscribe(pid \\ self()) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  def init(_args) do
    client = Phoenix.Sync.client!() |> dbg
    parent = self()

    {:ok, sync_task} =
      Task.start_link(fn ->
        for msg <- Electric.Client.stream(client, PhoenixSyncExample.Model, replica: :full) do
          GenServer.cast(parent, {:sync_message, msg})
        end
      end)

    {:ok, %{task: sync_task, subscribers: MapSet.new()}}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    Process.monitor(pid)
    new_subscribers = MapSet.put(state.subscribers, pid)
    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  def handle_cast({:sync_message, msg}, state) do
    for pid <- MapSet.to_list(state.subscribers) do
      send(pid, {:sync_message, msg})
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_subscribers = MapSet.delete(state.subscribers, pid)
    {:noreply, %{state | subscribers: new_subscribers}}
  end
end
