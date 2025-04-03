defmodule Phoenix.Sync.Application do
  @moduledoc false

  use Application

  require Logger

  @default_adapter Phoenix.Sync.Electric

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

  @doc false
  def config do
    Application.get_all_env(:phoenix_sync)
  end

  @doc false
  def adapter do
    config() |> adapter()
  end

  @doc false
  def adapter(opts) do
    Keyword.get(opts, :adapter, @default_adapter)
  end

  @doc false
  def children do
    config() |> children()
  end

  @doc false
  def children(opts) when is_list(opts) do
    {adapter, env} = adapter_env(opts)

    apply(adapter, :children, [env, opts])
  end

  @doc false
  def adapter_env(opts) do
    {
      adapter(),
      Keyword.get(opts, :env, :prod)
    }
  end

  @spec plug_opts() :: keyword()
  def plug_opts do
    config() |> plug_opts()
  end

  @doc false
  def plug_opts(opts) when is_list(opts) do
    {adapter, env} = adapter_env(opts)

    apply(adapter, :plug_opts, [env, opts])
  end

  @doc false
  def fetch_with_error(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, url} -> {:ok, url}
      :error -> {:error, "Missing required key #{inspect(key)}"}
    end
  end
end
