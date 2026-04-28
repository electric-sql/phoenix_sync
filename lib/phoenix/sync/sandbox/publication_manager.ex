if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.PublicationManager do
    @moduledoc false

    use GenServer

    def start_link(args) do
      GenServer.start_link(__MODULE__, args, name: name(args))
    end

    def name(stack_ref) do
      Electric.Replication.PublicationManager.name(stack_ref)
    end

    def init(opts) do
      {:ok, opts}
    end

    # intercept the snapshotter process's add_shape call to add it to the allow
    # list for the sandbox repo
    def handle_call({:add_shape, _shape_handle, _pub_filter}, {snapshotter, _ref}, state) do
      {:ok, owner} = Keyword.fetch(state, :owner)
      {:ok, repo} = Keyword.fetch(state, :repo)

      Ecto.Adapters.SQL.Sandbox.allow(repo, owner, snapshotter)

      {:reply, :ok, state}
    end

    def handle_call(_msg, _from, state) do
      {:reply, :ok, state}
    end
  end
end
