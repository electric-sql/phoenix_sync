defmodule Phoenix.Sync.Plug.Shapes do
  @moduledoc """
  A `Plug.Router` and `Phoenix.Router` compatible Plug handler that allows
  you to mount the full Electric shape api into your application.

  Unlike `Phoenix.Sync.Router.shape/2` this allows your app to serve
  shapes defined by `table` parameters, much like the Electric application.

  The advantage is that you're free to put your own authentication and
  authorization Plugs in front of this endpoint, integrating the auth for
  your shapes API with the rest of your app.

  ## Configuration

  Before configuring your router, you must install and configure the
  `:electric` application.

  See the documentation for [embedding electric](`Phoenix.Sync`) for
  details on embedding Electric into your Elixir application.

  ## Plug Integration

  Mount this Plug into your router using `Plug.Router.forward/2`.

      defmodule MyRouter do
        use Plug.Router, copy_opts_to_assign: :config
        use Phoenix.Sync.Plug.Shapes

        plug :match
        plug :dispatch

        forward "/shapes",
          to: Phoenix.Sync.Plug.Shapes,
          init_opts: [opts_in_assign: :config]
      end

  You **must** configure your `Plug.Router` with `copy_opts_to_assign` and
  pass the key you configure here (in this case `:config`) to the
  `Phoenix.Sync.Plug.Shapes` plug in it's `init_opts`.

  In your application, build your Electric confguration using `Electric.Application.api_plug_opts/0` and pass the result to your router as `electric`:

      # in application.ex
      def start(_type, _args) do
        electric_config = Electric.Application.api_plug_opts()

        children = [
          {Bandit, plug: {MyRouter, electric: electric_config}, port: 4000}
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

          forward "/", Phoenix.Sync.Plug.Shapes
        end
      end

  As for the Plug integration, include the Electric configuration at runtime
  within the `Application.start/2` callback.

      # in application.ex
      def start(_type, _args) do
        electric_config = Electric.Application.api_plug_opts()

        children = [
          # ...
          {MyAppWeb.Endpoint, electric: electric_config}
        ]

        Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
      end

  """

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

  @behaviour Plug

  def init(opts), do: Map.new(opts)

  def call(%{private: %{phoenix_endpoint: endpoint}} = conn, _config) do
    config = endpoint.config(:electric)

    api = Access.fetch!(config, :api)

    serve_api(conn, api)
  end

  def call(conn, %{opts_in_assign: key}) do
    api =
      get_in(conn.assigns, [key, :electric, :api]) ||
        raise "Unable to retrieve the Electric API configuration from the assigns"

    serve_api(conn, api)
  end

  @doc false
  def serve_api(conn, api) do
    conn = Plug.Conn.fetch_query_params(conn)

    Phoenix.Sync.Adapter.call(api, conn, conn.params)
  end
end
