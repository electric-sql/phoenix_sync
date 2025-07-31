defmodule Support.Emitter do
  use GenServer

  defstruct [:pid]

  alias __MODULE__

  def new do
    %Emitter{pid: ExUnit.Callbacks.start_supervised!(Emitter)}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def wait(pid) do
    GenServer.call(pid, :request, :infinity)
  end

  def emit(%Emitter{pid: pid}, events) when is_list(events) do
    GenServer.cast(pid, {:emit, events})
  end

  def init(_args) do
    {:ok, %{waiter: nil, buffer: []}}
  end

  def handle_call(:request, _from, %{buffer: [event | buffer]} = state) do
    {:reply, {:emit, event}, %{state | buffer: buffer}}
  end

  def handle_call(:request, from, %{buffer: []} = state) do
    {:noreply, %{state | waiter: from}}
  end

  def handle_cast({:emit, events}, %{waiter: waiter} = state) do
    [event | rest] = buffer = state.buffer ++ events

    if !is_nil(waiter) do
      :ok = GenServer.reply(waiter, {:emit, event})
      {:noreply, %{state | waiter: nil, buffer: rest}}
    else
      {:noreply, %{state | buffer: buffer}}
    end
  end

  defimpl Enumerable do
    def count(_emitter), do: {:error, __MODULE__}
    def member?(_emitter, _element), do: {:error, __MODULE__}
    def slice(_emitter), do: {:error, __MODULE__}

    def reduce(_emitter, {:halt, acc}, _fun) do
      {:halted, acc}
    end

    def reduce(emitter, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(emitter, &1, fun)}
    end

    def reduce(emitter, {:cont, acc}, fun) do
      {:emit, event} = Emitter.wait(emitter.pid)
      reduce(emitter, fun.(event, acc), fun)
    end
  end
end
