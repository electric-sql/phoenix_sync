defmodule Phoenix.Sync do
  @moduledoc """
  Real-time sync for Postgres-backed Phoenix applications.

  See the [docs](../../README.md) for more information.
  """

  alias Electric.Client.ShapeDefinition

  alias Phoenix.Sync.PredefinedShape

  @shape_keys [:namespace, :where, :columns]
  @shape_params @shape_keys |> Enum.map(&to_string/1)

  @type queryable() :: Ecto.Queryable.t() | Ecto.Schema.t() | Ecto.Changeset.t()
  @type shape_specification :: [
          unquote(NimbleOptions.option_typespec(Phoenix.Sync.PredefinedShape.schema()))
        ]
  @type shape_definition ::
          String.t()
          | queryable()
          | shape_specification()
  @type param_override ::
          {:namespace, String.t()}
          | {:table, String.t()}
          | {:where, String.t()}
          | {:columns, String.t()}
  @type param_overrides :: [param_override()]

  @type match_shape_params() :: %{
          table: String.t(),
          namespace: nil | String.t(),
          where: nil | String.t(),
          params: nil | %{String.t() => String.t()},
          columns: nil | [String.t(), ...]
        }

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

  @doc """
  Interrupts all long-polling requests matching the given shape definition.

  The broader the shape definition, the more requests will be interrupted.

  Returns the number of interrupted requests.

  ### Examples

  To interrupt all shapes on the `todos` table:

      Phoenix.Sync.interrupt("todos")
      Phoenix.Sync.interrupt(table: "todos")

  or the same using an `Ecto.Schema` module:

      Phoenix.Sync.interrupt(Todos.Todo)

  all shapes with the given parameterized where clause:

      Phoenix.Sync.interrupt(table: "todos", where: "user_id = $1")

  or a single shape for the given user:

      Phoenix.Sync.interrupt(
        from(t in Todos.Todo, where: t.user_id == ^user_id)
      )

      # or

      Phoenix.Sync.interrupt(
        table: "todos",
        where: "user_id = $1",
        params: [user_id]
      )

      # or

      Phoenix.Sync.interrupt(
        table: "todos",
        where: "user_id = '\#{user_id}'"
      )

  If you want more control over the match, you can pass a function that will
  receive a normalized shape definition and should return `true` if the active
  shape matches.

        Phoenix.Sync.interrupt(fn %{table: _, where: _, params: _} = shape ->
          shape.table == "todos" &&
            shape.where == "user_id = $1" &&
            shape.params["0"] == user_id
        end)

  The normalized shape argument is a map with the following keys:

  - `table`, e.g. `"todos"`
  - `namespace`, e.g. `"public"`
  - `where`, e.g. `"where user_id = $1"`
  - `params`, a map of argument position to argument value, e.g. `%{"0" => "true", "1" => "..."}`
  - `columns`, e.g. `["id", "title"]`

  All except `table` may be `nil`.

  ### Interrupting Ecto Query-based Shapes

  Be careful when mixing `Ecto` query-based shapes with interrupt calls using
  hand-written where clauses.

  The shape

      Phoenix.Sync.Controller.sync_stream(conn, params, fn ->
        from(t in Todos.Todo, where: t.user_id == ^user_id)
      end)

  will **not** be interrupted by

      Phoenix.Sync.interrupt(
        table: "todos",
        where: "user_id = '\#{user_id}'"
      )

  because the where clause matching is a simple *exact string* match and `Ecto` query
  generated where clauses will generally be different from the equivalent
  hand-written version. If you want to interrupt a query-based shape you should
  use the same query as the interrupt criteria.

  > #### Writing interrupts {: .tip}
  >
  > It's better to be too broad with your interrupt calls than too narrow.
  > Only clients whose shape definition changes after the `interrupt/1` call
  > will be affected.

  ## Supported options

  The more options you give the more specific the interrupt call will be. Only
  the table name is required.

  - `table` - Required. Interrupts all shapes matching the given table. E.g. `"todos"`
  - `namespace` - The table namespace. E.g. `"public"`
  - `where` - The shape's where clause. Can in be parameterized and will match
    all shapes with the same where filter irrespective of the parameters (unless
    provided). E.g. `"status = $1"`, `"completed = true"`
  - `columns` - The columns included in the shape. E.g. `["id", "title", "completed"]`
  - `params` - The values associated with a parameterized where clause. E.g. `[true, 1, "alive"]`, `%{1 => true}`
  """
  @spec interrupt(shape_definition() | (match_shape_params() -> boolean()), shape_options()) ::
          {:ok, non_neg_integer()}
  def interrupt(shape, shape_opts \\ []) do
    Phoenix.Sync.ShapeRequestRegistry.interrupt_matching(shape, shape_opts)
  end

  @doc """
  Returns a shape definition for the given params.

  ## Examples

  - An `Ecto.Schema` module:

        Phoenix.Sync.shape!(MyPlugApp.Todos.Todo)

  - An `Ecto` query:

        Phoenix.Sync.shape!(from(t in Todos.Todo, where: t.owner_id == ^user_id))

  - A `changeset/1` function which defines the table and columns:

        Phoenix.Sync.shape!(&Todos.Todo.changeset/1)

  - A `changeset/1` function plus a where clause:

        Phoenix.Sync.shape!(
          &Todos.Todo.changeset/1,
          where: "completed = false"
        )

    or a parameterized where clause:

        Phoenix.Sync.shape!(
          &Todos.Todo.changeset/1,
          where: "completed = $1", params: [false]
        )

  - A keyword list defining the shape parameters:

        Phoenix.Sync.shape!(
          table: "todos",
          namespace: "my_app",
          where: "completed = $1",
          params: [false]
        )

  ## Options

  When defining a shape via a keyword list, it supports the following options:

  #{NimbleOptions.docs(PredefinedShape.schema())}
  """
  @spec shape!(shape_definition(), shape_options()) :: PredefinedShape.t()
  def shape!(shape, shape_opts \\ []) do
    PredefinedShape.new!(shape, shape_opts)
  end
end
