defmodule Phoenix.Sync.Router do
  @moduledoc """
  Provides router macros to simplify the exposing of Electric shape streams
  within your Phoenix or Plug application.

  ## Phoenix Integration

  When using within a Phoenix application, you should just import the macros
  defined here

      import #{__MODULE__}

  ## Plug Integration

  Within Plug applications you need to do a little more work.



  **Note:** If you use Ecto queries in your shape definitions, e.g.

      shape "/todos",
        Ecto.Query.from(t in MyApp.Todos, where: t.completed == false)

  and you get the error `error: undefined variable "t"` it's because you forgot
  to require the Ecto.Query module. Above your shape route add:

      require Ecto.Query

  """

  import Phoenix.Sync.Plug.Utils

  # The reason to require `use` for the plug version is so that we can do some
  # validation of our environment, specifically we need the
  # `:copy_opts_to_assign` option to be set so we receive the
  # runtime-configured electric config in our plug without having to call the
  # api configuration function on every request
  defmacro __using__(opts \\ []) do
    # validate that we're being used in the context of a Plug.Router impl
    Phoenix.Sync.Plug.Utils.env!(__CALLER__)

    quote do
      # save this config value for use in our route/2 quoted expression
      @plug_assign_opts Phoenix.Sync.Plug.Utils.opts_in_assign!(
                          unquote(opts),
                          __MODULE__,
                          Phoenix.Sync.Router
                        )

      import Phoenix.Sync.Router
    end
  end

  defmacro sync(path, opts) when is_list(opts) do
    route(env!(__CALLER__), path, build_definition(path, __CALLER__, opts))
  end

  # e.g. shape "/path", Ecto.Query.from(t in MyTable)
  defmacro sync(path, queryable) when is_tuple(queryable) do
    route(env!(__CALLER__), path, build_shape_from_query(queryable, __CALLER__, []))
  end

  # e.g. shape "/path", Ecto.Query.from(t in MyTable), replica: :full
  defmacro sync(path, queryable, opts) when is_tuple(queryable) and is_list(opts) do
    route(env!(__CALLER__), path, build_shape_from_query(queryable, __CALLER__, opts))
  end

  defp route(:plug, path, definition) do
    quote do
      Plug.Router.match(unquote(path),
        via: :get,
        to: Phoenix.Sync.Router.Shape,
        init_opts: %{
          shape: unquote(Macro.escape(definition)),
          plug_opts_assign: @plug_assign_opts
        }
      )
    end
  end

  defp route(:phoenix, path, definition) do
    quote do
      Phoenix.Router.match(
        :get,
        unquote(path),
        Phoenix.Sync.Router.Shape,
        %{shape: unquote(Macro.escape(definition))},
        []
      )
    end
  end

  defp build_definition(path, caller, opts) when is_list(opts) do
    case Keyword.fetch(opts, :query) do
      {:ok, queryable} ->
        build_shape_from_query(queryable, caller, opts)

      :error ->
        define_shape(path, caller, opts)
    end
  end

  defp build_shape_from_query(queryable, caller, opts) do
    case Macro.expand_literals(queryable, %{caller | function: {:sync, 4}}) do
      schema when is_atom(schema) ->
        {storage, _binding} = Code.eval_quoted(opts[:storage], [], caller)

        Phoenix.Sync.PredefinedShape.new!(
          schema,
          Keyword.merge(opts, storage: storage)
        )

      query when is_tuple(query) ->
        raise ArgumentError,
          message:
            "Router shape configuration only accepts a Ecto.Schema module as a query. For Ecto.Query support please use `Phoenix.Sync.Controller.sync_render/3`"
    end
  end

  defp define_shape(path, caller, opts) do
    relation = build_relation(path, opts)

    {storage, _binding} = Code.eval_quoted(opts[:storage], [], caller)

    Phoenix.Sync.PredefinedShape.new!(Keyword.merge(opts, relation), storage: storage)
  end

  defp build_relation(path, opts) do
    case Keyword.fetch(opts, :table) do
      {:ok, table} ->
        [table: table]

      :error ->
        raise ArgumentError,
          message:
            "No valid table specified. The path #{inspect(path)} is not a valid table name and no `:table` option passed."
    end
    |> add_namespace(opts)
  end

  defp add_namespace(table, opts) do
    Keyword.merge(table, Keyword.take(opts, [:namespace]))
  end

  defmodule Shape do
    @behaviour Plug

    def init(opts), do: opts

    def call(%{private: %{phoenix_endpoint: endpoint}} = conn, %{shape: shape}) do
      config = endpoint.config(:electric)
      api = Keyword.fetch!(config, :api)

      serve_shape(conn, api, shape)
    end

    def call(conn, %{shape: shape, plug_opts_assign: assign_key}) do
      api =
        get_in(conn.assigns, [assign_key, :electric, :api]) ||
          raise RuntimeError,
            message:
              "Please configure your Router opts with [electric: Electric.Shapes.Api.plug_opts()]"

      serve_shape(conn, api, shape)
    end

    defp serve_shape(conn, api, shape) do
      {:ok, shape_api} = Phoenix.Sync.Adapter.predefined_shape(api, shape)

      conn = Plug.Conn.fetch_query_params(conn)
      Phoenix.Sync.Adapter.call(shape_api, conn, conn.params)
    end
  end
end
