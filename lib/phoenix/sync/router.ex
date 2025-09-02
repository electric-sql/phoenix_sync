defmodule Phoenix.Sync.Router do
  @moduledoc """
  Provides router macros to simplify the exposing of Electric shape streams
  within your Phoenix or Plug application.

  ## Phoenix Integration

  When using within a Phoenix application, you should just import the macros
  defined here in your `Phoenix.Router` module:

      defmodule MyAppWeb.Router do
        use Phoenix.Router

        import #{__MODULE__}

        scope "/shapes" do
          sync "/all-todos", MyApp.Todos.Todo

          sync "/pending-todos", MyApp.Todos.Todo,
            where: "completed = false"
        end
      end

  ## Plug Integration

  Within your `Plug.Router` module, `use #{__MODULE__}` and then
  add your `sync` routes:

      defmodule MyApp.Plug.Router do
        use Plug.Router, copy_opts_to_assign: :options
        use #{__MODULE__}

        plug :match
        plug :dispatch

        sync "/shapes/all-todos", MyApp.Todos.Todo

        sync "/shapes/pending-todos", MyApp.Todos.Todo,
          where: "completed = false"
      end

  You **must** use the `copy_opts_to_assign` option in `Plug.Router` in order
  for the `sync` macro to get the configuration defined in your
  `application.ex` [`start/2`](`c:Application.start/2`) callback.

  ## Transforms

  You can add a `transform` function to your shapes as explained in
  [`Phoenix.Sync.shape/2`](`Phoenix.Sync#shape!/2-transforms`) but, because `sync/2` and `sync/3` are
  macros, you need to use the `{module, function, args}` form when declaring
  the `transform` function.

        defmodule MyApp.Router do
          use Plug.Router, copy_opts_to_assign: :options

          # ...

          sync "/shapes/pending-todos", MyApp.Todos.Todo,
            where: "completed = false",
            transform: {MyApp.Router, :transform_todo, ["[PENDING]"]}

          def transform_todo(msg, prefix) do
            Map.update!(msg, "values", fn todo ->
              Map.put(todo, "title", prefix <> " " <> todo["title"])
            end)
          end
        end
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

  @doc """
  Defines a synchronization route for streaming Electric shapes.

  The shape can be defined in several ways:

  ### Using Ecto Schemas

  Defines a synchronization route for streaming Electric shapes using an Ecto schema.

      sync "/all-todos", MyApp.Todo

  Note: Only Ecto schema modules are supported as direct arguments. For Ecto queries,
  use the `query` option in the third argument or use `Phoenix.Sync.Controller.sync_render/3`.

  ### Using Ecto Schema and `where` clause

      sync "/incomplete-todos", MyApp.Todo, where: "completed = false"

  ### Using an explicit `table`

      sync "/incomplete-todos", table: "todos", where: "completed = false"


  See [the section on Shape definitions](readme.html#shape-definitions) for
  more details on keyword-based shapes.
  """
  defmacro sync(path, opts) when is_list(opts) do
    route(env!(__CALLER__), path, define_shape(opts, [], __CALLER__))
  end

  # e.g. shape "/path", Ecto.Query.from(t in MyTable)
  defmacro sync(path, queryable) when is_tuple(queryable) do
    route(env!(__CALLER__), path, define_shape(queryable, [], __CALLER__))
  end

  @doc """
  Create a synchronization route from an `Ecto.Schema` plus shape options.

      sync "/my-shape", MyApp.Todos.Todo,
        where: "completed = false"

  See `sync/2`.
  """
  # e.g. shape "/path", Ecto.Query.from(t in MyTable), replica: :full
  defmacro sync(path, queryable, opts) when is_tuple(queryable) and is_list(opts) do
    route(
      env!(__CALLER__),
      path,
      define_shape(queryable, opts, __CALLER__)
    )
  end

  defp route(:plug, path, definition) do
    quote bind_quoted: [path: path, shape: Macro.escape(definition)] do
      Plug.Router.match(path,
        via: :get,
        to: Phoenix.Sync.Router.Shape,
        init_opts: %{
          plug_opts_assign: @plug_assign_opts,
          shape: shape
        }
      )
    end
  end

  defp route(:phoenix, path, definition) do
    quote bind_quoted: [path: path, shape: Macro.escape(definition)] do
      Phoenix.Router.match(
        :get,
        path,
        Phoenix.Sync.Router.Shape,
        %{shape: shape},
        alias: false
      )
    end
  end

  defp define_shape(shape, opts, caller) do
    Phoenix.Sync.PredefinedShape.new_macro!(shape, opts, caller,
      context: env!(caller),
      function: {:sync, 3}
    )
  end

  defmodule Shape do
    @moduledoc false

    @behaviour Plug

    def init(opts), do: opts

    def call(%{private: %{phoenix_endpoint: endpoint}} = conn, %{shape: shape}) do
      api = endpoint.config(:phoenix_sync)

      serve_shape(conn, api, shape)
    end

    def call(conn, %{shape: shape, plug_opts_assign: assign_key}) do
      api =
        get_in(conn.assigns, [assign_key, :phoenix_sync]) ||
          raise RuntimeError,
            message:
              "Please configure your Router opts with [phoenix_sync: Phoenix.Sync.plug_opts()]"

      serve_shape(conn, api, shape)
    end

    defp serve_shape(conn, api, shape) do
      Phoenix.Sync.Electric.api_predefined_shape(conn, api, shape, fn conn, shape_api ->
        conn =
          conn
          |> Plug.Conn.fetch_query_params()
          |> Phoenix.Sync.Plug.CORS.call()

        Phoenix.Sync.Adapter.PlugApi.call(shape_api, conn, conn.params)
      end)
    end
  end
end
