defmodule Phoenix.Sync.Application do
  use Application

  require Logger

  @env Mix.env()

  @impl true
  def start(_type, _args) do
    case children() do
      {:ok, children} ->
        Supervisor.start_link(children, strategy: :one_for_one, name: Phoenix.Sync.Supervisor)

      {:error, reason} ->
        Logger.warning(reason)
        Supervisor.start_link([], strategy: :one_for_one, name: Phoenix.Sync.Supervisor)
    end
  end

  def config do
    Application.get_all_env(:phoenix_sync)
  end

  def children do
    config() |> children()
  end

  def children(opts) when is_list(opts) do
    children(@env, opts)
  end

  def children(env, opts) do
    adapter = Keyword.get(opts, :adapter, Phoenix.Sync.Electric)

    apply(adapter, :children, [env, opts])
  end

  def plug_opts do
    config() |> plug_opts()
  end

  def plug_opts(opts) when is_list(opts) do
    plug_opts(@env, opts)
  end

  def plug_opts(env, opts) do
    adapter = Keyword.get(opts, :adapter, Phoenix.Sync.Electric)

    apply(adapter, :plug_opts, [env, opts])
  end

  def fetch_with_error(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, url} -> {:ok, url}
      :error -> {:error, "Missing required key #{inspect(key)}"}
    end
  end
end
