defmodule Phoenix.Sync do
  @moduledoc """
  Real-time sync for Postgres-backed Phoenix applications.

  See the [docs](../../README.md) for more information.
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
          | shape_specification()
  @type param_override ::
          {:namespace, String.t()}
          | {:table, String.t()}
          | {:where, String.t()}
          | {:columns, String.t()}
  @type param_overrides :: [param_override()]

  @doc """
  Returns the required adapter configuration for your Phoenix Endpoint or
  `Plug.Router`.

  ## Phoenix

  Configure your endpoint with the configuration at runtime by passing the
  `phoenix_sync` configuration to your endpoint in the `Application.start/2`
  callback:

      def start(_type, _args) do
        children = [
          # ...
          {MyAppWeb.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
        ]
      end

  ## Plug

  Add the configuration to the Plug opts in your server configuration:

      children = [
        {Bandit, plug: {MyApp.Router, phoenix_sync: Phoenix.Sync.plug_opts()}}
      ]

  Your `Plug.Router` must be configured with
  [`copy_opts_to_assign`](https://hexdocs.pm/plug/Plug.Builder.html#module-options) and you should `use` the rele

      defmodule MyApp.Router do
        use Plug.Router, copy_opts_to_assign: :options

        use Phoenix.Sync.Controller
        use Phoenix.Sync.Router

        plug :match
        plug :dispatch

        sync "/shapes/todos", Todos.Todo

        get "/shapes/user-todos" do
          %{"user_id" => user_id} = conn.params
          sync_render(conn, from(t in Todos.Todo, where: t.owner_id == ^user_id)
        end
      end
  """
  defdelegate plug_opts(), to: Phoenix.Sync.Application

  @doc false
  defdelegate plug_opts(config), to: Phoenix.Sync.Application

  defdelegate client!(), to: Phoenix.Sync.Client, as: :new!

  _ = """
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

  @doc false
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
