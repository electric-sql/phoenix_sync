defmodule Phoenix.Sync.Sandbox.ExpiryManager do
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Electric.ShapeCache.ExpiryManager.name(args))
  end

  def init(_) do
    {:ok, []}
  end
end
