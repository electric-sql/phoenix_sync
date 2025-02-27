defmodule Phoenix.Sync.Application do
  use Application

  require Logger

  @env Mix.env()
  @valid_modes [:http, :embedded, :disabled]

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

  def valid_modes, do: @valid_modes

  def children do
    config()
    |> children()
  end

  def children(opts) when is_list(opts) do
    children(@env, opts)
  end

  def children(env, opts) do
    case Keyword.fetch(opts, :mode) do
      {:ok, mode} when mode in @valid_modes ->
        embedded_children(env, mode, opts)

      {:ok, invalid_mode} ->
        {:error,
         "Invalid mode `#{inspect(invalid_mode)}`. Valid modes are: #{Enum.map_join(@valid_modes, " or ", &"`:#{&1}`")}"}

      :error ->
        if electric_available?() do
          Logger.warning([
            "missing mode configuration for phoenix_sync. Electric is installed so assuming `embedded` mode"
          ])

          embedded_children(env, :embedded, opts)
        else
          {:error,
           "Missing `mode`. Should be one of #{Enum.map_join(@valid_modes, " or ", &":#{&1}")}"}
        end
    end
  end

  def plug_opts do
    config()
    |> plug_opts()
  end

  def plug_opts(opts) when is_list(opts) do
    plug_opts(@env, opts)
  end

  def plug_opts(env, opts) do
    # don't need to validate the mode here -- it will have already been
    # validated by children/0 which is run at phoenix_sync startup before the
    # plug opts call even comes through
    case Keyword.fetch(opts, :mode) do
      {:ok, mode} when mode in @valid_modes ->
        plug_opts(env, mode, opts)

      {:ok, mode} ->
        raise ArgumentError, message: "Invalid `mode` setting: #{inspect(mode)}"

      :error ->
        if electric_available?() do
          Logger.warning([
            "missing mode configuration for phoenix_sync. Electric is installed so assuming `embedded` mode"
          ])

          plug_opts(env, :embedded, opts)
        else
          raise ArgumentError, message: "Missing `mode` configuration"
        end
    end
  end

  if Code.ensure_loaded?(Electric.Application) do
    def api_opts do
      config()
      |> api_opts()
    end

    def api_opts(opts) when is_list(opts) do
      api_opts(@env, opts)
    end

    def api_opts(env, opts) do
      electric_opts = Keyword.get(opts, :electric, [])
      core_configuration(env, electric_opts)
    end
  end

  defp plug_opts(_env, :http, opts) do
    case Keyword.pop(opts, :electric, nil) do
      {nil, _} ->
        raise ArgumentError, message: "Missing :electric configuration"

      {electric_opts, _sync_opts} when is_list(electric_opts) ->
        case http_mode_plug_opts(electric_opts) do
          {:ok, config} -> config
          {:error, reason} -> raise ArgumentError, message: reason
        end
    end
  end

  defp plug_opts(env, :embedded, opts) do
    if electric_available?() do
      electric_opts = Keyword.get(opts, :electric, [])

      env
      |> core_configuration(electric_opts)
      |> Electric.Application.api_plug_opts()
    else
      raise RuntimeError,
        message: "Configured for embedded mode but `:electric` dependency not installed"
    end
  end

  defp embedded_children(_env, :disabled, _opts) do
    {:ok, []}
  end

  defp embedded_children(env, mode, opts) do
    case Keyword.pop(opts, :electric, nil) do
      {nil, _} ->
        {:error, "Missing :electric configuration"}

      {electric_opts, _sync_opts} when is_list(electric_opts) ->
        electric_children(env, mode, electric_opts)
    end
  end

  defp electric_children(env, mode, opts) do
    case validate_database_config(env, mode, opts) do
      {:start, db_config_fun, message} ->
        if electric_available?() do
          db_config =
            db_config_fun.()
            |> Keyword.update!(:connection_opts, &Electric.Utils.obfuscate_password/1)

          electric_config = core_configuration(env, db_config)

          Logger.info(message)

          http_server =
            case mode do
              :http -> electric_api_server(electric_config)
              :embedded -> []
            end

          {:ok,
           [
             {Electric.StackSupervisor, Electric.Application.configuration(electric_config)}
             | http_server
           ]}
        else
          {:error,
           "Electric configured to start in embedded mode but :electric dependency not available"}
        end

      :ignore ->
        {:ok, []}

      {:error, _} = error ->
        error
    end
  end

  defp core_configuration(env, opts) do
    opts
    |> env_defaults(env)
    |> Keyword.put_new(:stack_id, "electric-embedded")
  end

  defp env_defaults(opts, :dev) do
    Keyword.put_new(
      opts,
      :storage_dir,
      Path.join(System.tmp_dir!(), "electric/shape-data#{System.monotonic_time()}")
    )
  end

  defp env_defaults(opts, :test) do
    stack_id = "electric-stack#{System.monotonic_time()}"

    opts = Keyword.put_new(opts, :stack_id, stack_id)

    opts
    |> Keyword.put_new(
      :storage,
      {Electric.ShapeCache.InMemoryStorage,
       table_base_name: :"electric-storage#{opts[:stack_id]}", stack_id: opts[:stack_id]}
    )
    |> Keyword.put_new(
      :persistent_kv,
      {Electric.PersistentKV.Memory, :new!, []}
    )
  end

  defp env_defaults(opts, _) do
    opts
  end

  # Returns a function to generate the config so that we can
  # centralise the test for the existance of electric.
  # Need this because the convert_repo_config/1 function needs Electric
  # installed too
  defp validate_database_config(_env, mode, opts) do
    case Keyword.pop(opts, :repo, nil) do
      {nil, opts} ->
        case Keyword.fetch(opts, :connection_opts) do
          {:ok, connection_opts} when is_list(connection_opts) ->
            # TODO: validate reasonable connection opts?
            {:start, fn -> opts end,
             "Starting Electric replication stream from postgresql://#{connection_opts[:host]}:#{connection_opts[:port] || 5432}/#{connection_opts[:database]}"}

          :error ->
            case mode do
              :embedded ->
                {:error,
                 "No database configuration available. Include either a `repo` or a `connection_opts` in the phoenix_sync `electric` config"}

              :http ->
                :ignore
            end
        end

      {repo, opts} when is_atom(repo) ->
        if Code.ensure_loaded?(repo) && function_exported?(repo, :config, 0) do
          repo_config = apply(repo, :config, [])

          {:start,
           fn -> Keyword.put(opts, :connection_opts, convert_repo_config(repo_config)) end,
           "Starting Electric replication stream using #{repo} configuration"}
        else
          {:error, "#{inspect(repo)} is not a valid Ecto.Repo module"}
        end
    end
  end

  defp convert_repo_config(repo_config) do
    expected_keys = Electric.connection_opts_schema() |> Keyword.keys()

    ssl_opts =
      case Keyword.get(repo_config, :ssl, nil) do
        off when off in [nil, false] -> [sslmode: :disable]
        true -> [sslmode: :require]
        _opts -> []
      end

    tcp_opts =
      if :inet6 in Keyword.get(repo_config, :socket_options, []),
        do: [ipv6: true],
        else: []

    repo_config
    |> Keyword.take(expected_keys)
    |> Keyword.merge(ssl_opts)
    |> Keyword.merge(tcp_opts)
  end

  defp http_mode_plug_opts(electric_config) do
    with {:ok, url} <- fetch_with_error(electric_config, :url),
         credential_params = electric_config |> Keyword.get(:credentials, []) |> Map.new(),
         extra_params = electric_config |> Keyword.get(:params, []) |> Map.new(),
         params = Map.merge(extra_params, credential_params),
         {:ok, client} <-
           Electric.Client.new(
             base_url: url,
             params: params,
             fetch: {Electric.Client.Fetch.HTTP, [request: [raw: true]]}
           ) do
      {:ok, api: %Phoenix.Sync.Adapter.ElectricClient{client: client}}
    end
  end

  defp fetch_with_error(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, url} -> {:ok, url}
      :error -> {:error, "Missing required key #{inspect(key)}"}
    end
  end

  def electric_available? do
    Code.ensure_loaded?(Electric.Application)
  end

  defp electric_api_server(opts) do
    config = electric_http_config(opts)

    cond do
      Code.ensure_loaded?(Bandit) ->
        Electric.Application.api_server(Bandit, config)

      Code.ensure_loaded?(Plug.Cowboy) ->
        Electric.Application.api_server(Plug.Cowboy, config)

      true ->
        raise RuntimeError,
          message: "No HTTP server found. Please install either Bandit or Plug.Cowboy"
    end
  end

  defp electric_http_config(opts) do
    case Keyword.fetch(opts, :http) do
      {:ok, http_opts} ->
        opts
        |> then(fn o ->
          if(port = http_opts[:port], do: Keyword.put(o, :service_port, port), else: o)
        end)

      :error ->
        opts
    end
  end
end
