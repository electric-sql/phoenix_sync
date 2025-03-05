defmodule Phoenix.Sync.Electric do
  @moduledoc """
  A `Plug.Router` and `Phoenix.Router` compatible Plug handler that allows
  you to mount the full Electric shape api into your application.

  Unlike `Phoenix.Sync.Router.sync/2` this allows your app to serve
  shapes defined by `table` parameters, much like the Electric application.

  The advantage is that you're free to put your own authentication and
  authorization Plugs in front of this endpoint, integrating the auth for
  your shapes API with the rest of your app.

  ## Configuration

  Before configuring your router, you must install and configure the
  `:phoenix_sync` application.

  See the documentation for [embedding electric](`Phoenix.Sync`) for
  details on embedding Electric into your Elixir application.

  ## Plug Integration

  Mount this Plug into your router using `Plug.Router.forward/2`.

      defmodule MyRouter do
        use Plug.Router, copy_opts_to_assign: :config
        use Phoenix.Sync.Electric

        plug :match
        plug :dispatch

        forward "/shapes",
          to: Phoenix.Sync.Electric,
          init_opts: [opts_in_assign: :config]
      end

  You **must** configure your `Plug.Router` with `copy_opts_to_assign` and
  pass the key you configure here (in this case `:config`) to the
  `Phoenix.Sync.Electric` plug in it's `init_opts`.

  In your application, build your Electric confguration using
  `Phoenix.Sync.plug_opts()` and pass the result to your router as
  `phoenix_sync`:

      # in application.ex
      def start(_type, _args) do
        children = [
          {Bandit, plug: {MyRouter, phoenix_sync: Phoenix.Sync.plug_opts()}, port: 4000}
        ]

        Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
      end

  ## Phoenix Integration

  Use `Phoenix.Router.forward/2` in your router:

      defmodule MyAppWeb.Router do
        use Phoenix.Router

        pipeline :shapes do
          # your authz plugs
        end

        scope "/shapes" do
          pipe_through [:shapes]

          forward "/", Phoenix.Sync.Electric
        end
      end

  As for the Plug integration, include the configuration at runtime
  within the `Application.start/2` callback.

      # in application.ex
      def start(_type, _args) do
        children = [
          # ...
          {MyAppWeb.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
        ]

        Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
      end

  """

  import Phoenix.Sync.Application, only: [fetch_with_error: 2]

  require Logger

  @behaviour Phoenix.Sync.Adapter
  @behaviour Plug

  @valid_modes [:http, :embedded, :disabled]
  @client_valid_modes @valid_modes -- [:disabled]
  @electric_available? Code.ensure_loaded?(Electric.Application)

  defmacro __using__(_opts \\ []) do
    Phoenix.Sync.Plug.Utils.env!(__CALLER__)

    quote do
      Phoenix.Sync.Plug.Utils.opts_in_assign!(
        [],
        __MODULE__,
        Phoenix.Sync.Plug.Shapes
      )
    end
  end

  @doc false
  @impl Plug
  def init(opts), do: Map.new(opts)

  @doc false
  @impl Plug
  def call(%{private: %{phoenix_endpoint: endpoint}} = conn, _config) do
    api = endpoint.config(:phoenix_sync)

    serve_api(conn, api)
  end

  def call(conn, %{opts_in_assign: key}) do
    api =
      get_in(conn.assigns, [key, :phoenix_sync]) ||
        raise "Unable to retrieve the Electric API configuration from the assigns"

    serve_api(conn, api)
  end

  @doc false
  def serve_api(conn, api) do
    conn = Plug.Conn.fetch_query_params(conn)

    Phoenix.Sync.Adapter.PlugApi.call(api, conn, conn.params)
  end

  @doc false
  def valid_modes, do: @valid_modes

  @doc false
  @impl Phoenix.Sync.Adapter
  def children(env, opts) do
    {mode, electric_opts} = electric_opts(opts)

    case mode do
      :disabled ->
        {:ok, []}

      mode when mode in @valid_modes ->
        embedded_children(env, mode, electric_opts)

      invalid_mode ->
        {:error,
         "Invalid mode `#{inspect(invalid_mode)}`. Valid modes are: #{Enum.map_join(@valid_modes, " or ", &"`:#{&1}`")}"}
    end
  end

  @doc false
  @impl Phoenix.Sync.Adapter
  def plug_opts(env, opts) do
    {mode, electric_opts} = electric_opts(opts)
    # don't need to validate the mode here -- it will have already been
    # validated by children/0 which is run at phoenix_sync startup before the
    # plug opts call even comes through
    case mode do
      :disabled ->
        []

      mode when mode in @valid_modes ->
        plug_opts(env, mode, electric_opts)

      mode ->
        raise ArgumentError,
          message:
            "Invalid `mode` for phoenix_sync: #{inspect(mode)}. Valid modes are: #{Enum.map_join(@valid_modes, " or ", &"`:#{&1}`")}"
    end
  end

  @doc false
  @impl Phoenix.Sync.Adapter
  def client(opts) do
    {mode, electric_opts} = electric_opts(opts)

    case mode do
      mode when mode in @client_valid_modes ->
        electric_opts
        |> stack_id()
        |> configure_client(mode)

      invalid_mode ->
        {:error, "Cannot configure client for mode #{inspect(invalid_mode)}"}
    end
  end

  @doc false
  def electric_available? do
    @electric_available?
  end

  defp electric_opts(opts) do
    Keyword.pop_lazy(opts, :mode, fn ->
      if electric_available?() do
        Logger.warning([
          "missing mode configuration for :phoenix_sync. Electric is installed so assuming `embedded` mode"
        ])

        :embedded
      else
        Logger.warning("No `:mode` configuration for :phoenix_sync, assuming `:disabled`")

        :disabled
      end
    end)
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

  defp plug_opts(_env, :http, opts) do
    case http_mode_plug_opts(opts) do
      {:ok, config} -> config
      {:error, reason} -> raise ArgumentError, message: reason
    end
  end

  defp plug_opts(env, :embedded, electric_opts) do
    if electric_available?() do
      env
      |> core_configuration(electric_opts)
      |> Electric.Application.api_plug_opts()
      |> Keyword.fetch!(:api)
    else
      raise RuntimeError,
        message: "Configured for embedded mode but `:electric` dependency not installed"
    end
  end

  defp embedded_children(_env, :disabled, _opts) do
    {:ok, []}
  end

  defp embedded_children(env, mode, opts) do
    electric_children(env, mode, opts)
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

  defp stack_id(opts) do
    Keyword.put_new(opts, :stack_id, "electric-embedded")
  end

  defp core_configuration(env, opts) do
    opts
    |> env_defaults(env)
    |> stack_id()
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
      {:ok, %Phoenix.Sync.Electric.ClientAdapter{client: client}}
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

  if @electric_available? do
    defp configure_client(opts, :embedded) do
      Electric.Client.embedded(opts)
    end
  else
    defp configure_client(_opts, :embedded) do
      {:error, "electric not installed, unable to created embedded client"}
    end
  end

  defp configure_client(opts, :http) do
    case Keyword.fetch(opts, :url) do
      {:ok, url} ->
        Electric.Client.new(base_url: url)

      :error ->
        {:error, "`phoenix_sync[:electric][:url]` not set for phoenix_sync in HTTP mode"}
    end
  end
end

if Code.ensure_loaded?(Electric.Shapes.Api) do
  defimpl Phoenix.Sync.Adapter.PlugApi, for: Electric.Shapes.Api do
    alias Electric.Shapes

    alias Phoenix.Sync.PredefinedShape

    def predefined_shape(api, %PredefinedShape{} = shape) do
      Shapes.Api.predefined_shape(api, PredefinedShape.to_api_params(shape))
    end

    def call(api, %{method: "GET"} = conn, params) do
      case Shapes.Api.validate(api, params) do
        {:ok, request} ->
          conn
          |> Plug.Conn.assign(:request, request)
          |> Shapes.Api.serve_shape_log(request)

        {:error, response} ->
          conn
          |> Shapes.Api.Response.send(response)
          |> Plug.Conn.halt()
      end
    end

    def call(api, %{method: "DELETE"} = conn, params) do
      case Shapes.Api.validate_for_delete(api, params) do
        {:ok, request} ->
          conn
          |> Plug.Conn.assign(:request, request)
          |> Shapes.Api.delete_shape(request)

        {:error, response} ->
          conn
          |> Shapes.Api.Response.send(response)
          |> Plug.Conn.halt()
      end
    end

    def call(_api, %{method: "OPTIONS"} = conn, _params) do
      Shapes.Api.options(conn)
    end
  end
end
