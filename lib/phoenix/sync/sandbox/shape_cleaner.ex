if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.ShapeCleaner do
    @moduledoc false
    # Stub implementation for Electric.Shapes.Supervisor
    # The sandbox doesn't need actual shape cleaning

    use GenServer

    def child_spec(opts) do
      {:ok, stack_id} = Keyword.fetch(opts, :stack_id)

      %{
        id: {__MODULE__, stack_id},
        start: {__MODULE__, :start_link, [opts]},
        type: :worker,
        restart: :transient
      }
    end

    def start_link(opts) do
      stack_id = Keyword.fetch!(opts, :stack_id)
      GenServer.start_link(__MODULE__, stack_id, name: name(stack_id))
    end

    def name(stack_id) do
      Phoenix.Sync.Sandbox.name({__MODULE__, stack_id})
    end

    def init(stack_id) do
      {:ok, %{stack_id: stack_id}}
    end

    # No-op implementations for shape cleaner behavior
    def handle_cast(_msg, state), do: {:noreply, state}
    def handle_call(_msg, _from, state), do: {:reply, :ok, state}
  end
end
