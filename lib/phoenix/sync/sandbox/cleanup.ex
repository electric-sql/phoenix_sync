defmodule Phoenix.Sync.Sandbox.Cleanup do
  # We use PureFileStorage in the sandbox to remove compatibility issues
  # this ensures that the storage dir is removed when the stack is torn
  # down
  @moduledoc false

  use GenServer

  def start_link({stack_id, storage_dir}) do
    GenServer.start_link(__MODULE__, {stack_id, storage_dir})
  end

  @impl GenServer
  def init({stack_id, storage_dir}) do
    Process.flag(:trap_exit, true)
    {:ok, Path.join(storage_dir, stack_id)}
  end

  @impl GenServer
  def terminate(_reason, storage_dir) do
    File.rm_rf!(storage_dir)
    :ok
  end
end
