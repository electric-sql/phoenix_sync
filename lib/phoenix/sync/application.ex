defmodule Phoenix.Sync.Application do
  @moduledoc false

  use Application

  require Logger

  @default_adapter Phoenix.Sync.Electric

  @impl true
  def start(_type, _args) do
    base_children = [Phoenix.Sync.Shape.Supervisor, Phoenix.Sync.ShapeRequestRegistry]

    children =
      case children() do
        {:ok, children} ->
          children

        {:error, reason} ->
          Logger.warning(reason)
          []
      end

    Supervisor.start_link(base_children ++ children,
      strategy: :one_for_one,
      name: Phoenix.Sync.Supervisor
    )
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
    warn_missing_env(opts)

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

  defp warn_missing_env(config) do
    if config[:mode] != :disabled && !config[:env] do
      Logger.warning("""
      No `env` specified for :phoenix_sync: defaulting to `:prod`.

      Add the following to your config:

      config :phoenix_sync,
        env: config_env(),
        # the rest of your config

      In `:prod` mode, shapes are persisted between server restarts
      which may cause problems in `:dev` or `:test` environments.
      """)
    end

    config
  end
end
