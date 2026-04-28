defmodule Phoenix.Sync.Sandbox.InitializeStack do
  @moduledoc false

  # synchronously initializes the stack

  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl GenServer
  def init(args) do
    {:ok, stack_id} = Keyword.fetch(args, :stack_id)

    :ok = Electric.ShapeCache.ShapeStatusOwner.refresh(stack_id)

    Electric.LsnTracker.set_last_processed_lsn(
      stack_id,
      Electric.Postgres.Lsn.from_integer(0)
    )

    :ignore
  end
end
