defmodule Phoenix.Sync do
  @moduledoc """
  Wrappers to ease integration of [Electric’s Postgres syncing
  service](https://electric-sql.com) with [Phoenix
  applications](https://www.phoenixframework.org/).

  There are currently 2 integration modes: [`Phoenix.LiveView`
  streams](#module-phoenix-liveview-streams) and [configuration
  gateway](#module-configuration-gateway).

  ## Phoenix.LiveView Streams

  `Phoenix.Sync.LiveView.electric_stream/4` integrates with
  [`Phoenix.LiveView.stream/4`](https://hexdocs.pm/phoenix_live_view/Phoenix.LiveView.html#stream/4)
  and provides a live updating collection of items.

  ## Configuration Gateway

  Using `Phoenix.Sync.Plug` you can create endpoints that
  return configuration information for your Electric Typescript clients. See
  [that module's documentation](`Phoenix.Sync.Plug`) for
  more information.

  ## Installation

  Add `phoenix_sync` to your application dependencies:

      def deps do
        [
          {:phoenix_sync, "~> 0.1"}
        ]
      end

  ## Configuration

  In your `config/config.exs` or `config/runtime.exs` you **must** configure the
  client for the Electric streaming API:

      import Config

      config :phoenix_sync, Electric.Client,
        # one of `base_url` or `endpoint` is required
        base_url: System.get_env("ELECTRIC_URL", "http://localhost:3000"),
        # endpoint: System.get_env("ELECTRIC_ENDPOINT", "http://localhost:3000/v1/shape"),
        # optional
        database_id: System.get_env("ELECTRIC_DATABASE_ID", nil)

  See the documentation for [`Electric.Client.new/1`](`Electric.Client.new/1`)
  for information on the client configuration.

  ## Embedding Electric

  **TODO**
  """

  alias Electric.Client.ShapeDefinition

  @shape_keys [:namespace, :where, :columns]
  @shape_params @shape_keys |> Enum.map(&to_string/1)

  @type shape_specification :: [
          unquote(NimbleOptions.option_typespec(Phoenix.Sync.PredefinedShape.schema()))
        ]
  @type shape_definition ::
          String.t()
          | Ecto.Queryable.t()
          | shape_definition()
  @type param_override ::
          {:namespace, String.t()}
          | {:table, String.t()}
          | {:where, String.t()}
          | {:columns, String.t()}
  @type param_overrides :: [param_override()]

  defdelegate plug_opts(), to: Phoenix.Sync.Application
  defdelegate plug_opts(opts), to: Phoenix.Sync.Application

  @doc """
  Create a new `Electric.Client` instance based on the application config.

  To connect your live streams to an externally hosted Electric instance over
  HTTP, configure your app with the URL of the Electric server:

    # dev.exs
    config :phoenix_sync, Electric.Client,
      base_url: "http://localhost:3000"

  If Electric is installed as a dependency of your app, and you wish to connect
  your live streams to this internal application, then you don't need to configure
  anything -- `client!/0` will return an `Electric.Client` instance configured to
  data directly from the running Electric application.

  See [`Electric.Client.new/1`](`Electric.Client.new/1`) for the available
  options.

  """
  defdelegate client!, to: Phoenix.Sync.Client, as: :new!

  @doc """
  Use request query parameters to create a `Electric.Client.ShapeDefinition`.

  Useful when creating authorization endpoints that validate a user's access to
  a specific shape.

  ## Parameters

  ### Required

  - `table` - the Postgres [table name](https://electric-sql.com/docs/guides/shapes#table)

    Note: `table` is not required in the parameters if a `:table` override is set.

  ### Optional

  - `where` - the [Shape's where clause](https://electric-sql.com/docs/guides/shapes#where-clause)
  - `columns` - The columns to include in the shape.
  - `namespace` - The Postgres namespace (also called `SCHEMA`).

  See
  [`Electric.Client.ShapeDefinition.new/2`](`Electric.Client.ShapeDefinition.new/2`)
  for more details on the parameters.

  ### Examples

      # pass the Plug.Conn struct for a request
      iex> Phoenix.Sync.shape_from_params(%Plug.Conn{params: %{"table" => "items", "where" => "visible = true" }})
      {:ok, %Electric.Client.ShapeDefinition{table: "items", where: "visible = true"}}

      # or a simple parameter map
      iex> Phoenix.Sync.shape_from_params(%{"table" => "items", "columns" => "id,name,value" })
      {:ok, %Electric.Client.ShapeDefinition{table: "items", columns: ["id", "name", "value"]}}

      iex> Phoenix.Sync.shape_from_params(%{"columns" => "id,name,value" })
      {:error, "Missing `table` parameter"}

  ## Overriding Parameter Values

  If you want to hard-code some elements of the shape, ignoring the values from
  the request, or to set defaults, then use the `overrides` to set specific
  values for elements of the shape.

  ### Examples

      iex> Phoenix.Sync.shape_from_params(%{"columns" => "id,name,value"}, table: "things")
      {:ok, %Electric.Client.ShapeDefinition{table: "things", columns: ["id", "name", "value"]}}

      iex> Phoenix.Sync.shape_from_params(%{"table" => "ignored"}, table: "things")
      {:ok, %Electric.Client.ShapeDefinition{table: "things"}}

  """
  @spec shape_from_params(Plug.Conn.t() | Plug.Conn.params(), overrides :: param_overrides()) ::
          {:ok, Electric.Client.ShapeDefinition.t()} | {:error, String.t()}

  def shape_from_params(conn_or_map, overrides \\ [])

  def shape_from_params(%Plug.Conn{} = conn, overrides) do
    %{params: params} = Plug.Conn.fetch_query_params(conn)
    shape_from_params(params, overrides)
  end

  def shape_from_params(params, overrides) when is_map(params) do
    shape_params =
      params
      |> Map.take(@shape_params)
      |> Map.new(fn
        {"columns", ""} ->
          {:columns, nil}

        {"columns", v} when is_binary(v) ->
          {:columns, :binary.split(v, ",", [:global, :trim_all])}

        {k, v} ->
          {String.to_existing_atom(k), v}
      end)

    if table = Keyword.get(overrides, :table, Map.get(params, "table")) do
      ShapeDefinition.new(
        table,
        Enum.map(@shape_keys, fn k ->
          {k, Keyword.get(overrides, k, Map.get(shape_params, k))}
        end)
      )
    else
      {:error, "Missing `table` parameter"}
    end
  end
end
