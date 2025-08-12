if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
  defmodule Phoenix.Sync.Sandbox.PublicationManager do
    @moduledoc false

    use GenServer

    @behaviour Electric.Replication.PublicationManager

    def start_link(_) do
      :ignore
    end

    def init(_arg) do
      :ignore
    end

    def name(stack_id) when is_binary(stack_id) do
      Phoenix.Sync.Sandbox.name({__MODULE__, stack_id})
    end

    def name(opts) when is_list(opts) do
      opts
      |> Keyword.fetch!(:stack_id)
      |> name()
    end

    def recover_shape(_shape_handle, _shape, _opts) do
      :ok
    end

    def recover_shape(_shape, _opts) do
      :ok
    end

    def add_shape(_shape_handle, _shape, opts) do
      snapshotter = self()
      {:ok, owner} = Keyword.fetch(opts, :owner)
      {:ok, repo} = Keyword.fetch(opts, :repo)

      Ecto.Adapters.SQL.Sandbox.allow(repo, owner, snapshotter)
      :ok
    end

    def add_shape(_shape, _opts) do
      :ok
    end

    def remove_shape(_shape_handle, _shape, _opts) do
      :ok
    end

    def remove_shape(_shape, _opts) do
      :ok
    end

    def refresh_publication(_opts) do
      :ok
    end
  end
end
