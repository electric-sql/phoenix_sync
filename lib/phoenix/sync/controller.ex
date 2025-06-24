defmodule Phoenix.Sync.Controller do
  @moduledoc """
  Provides controller-level integration with sync streams.

  Unlike `Phoenix.Sync.Router.sync/2`, which only permits static shape
  definitions, in a controller you can use request and session information to
  filter your data.

  ## Phoenix Example

      defmodule MyAppWeb.TodoController do
        use Phoenix.Controller, formats: [:html, :json]

        import #{__MODULE__}

        alias MyApp.Todos

        def all(conn, %{"user_id" => user_id} = params) do
          sync_render(
            conn,
            params,
            from(t in Todos.Todo, where: t.owner_id == ^user_id)
          )
        end
      end

  ## Plug Example

  You should `use #{__MODULE__}` in your `Plug.Router`, then within your route
  you can use the `sync_render/2` function.

      defmodule MyPlugApp.Router do
        use Plug.Router, copy_opts_to_assign: :options
        use #{__MODULE__}

        plug :match
        plug :dispatch

        get "/todos" do
          sync_render(conn, MyPlugApp.Todos.Todo)
        end
      end

  ## Shape definitions

  Shape definitions can be any of the following:

  - An `Ecto.Schema` module:

        sync_render(conn, MyPlugApp.Todos.Todo)

  - An `Ecto` query:

        sync_render(conn, params, from(t in Todos.Todo, where: t.owner_id == ^user_id))

  - A `changeset/1` function which defines the table and columns:

        sync_render(conn, params, &Todos.Todo.changeset/1)

  - A `changeset/1` function plus a where clause:

        sync_render(conn, params, &Todos.Todo.changeset/1, where: "completed = false")

    or a parameterized where clause:

        sync_render(conn, params, &Todos.Todo.changeset/1, where: "completed = $1", params: [false])

  - A keyword list defining the shape parameters:

        sync_render(conn, params, table: "todos", namespace: "my_app", where: "completed = $1", params: [false])

  ## Interruptible calls

  There may be circumstances where shape definitions are dynamic based on, say, a database query.

  For instance, when creating a task manager apps your clients will have a list
  of tasks and each task will have a set of steps.

  So the controller code the `steps` sync endpoint might look like this:

      def steps(conn, %{"user_id" => user_id} = params) do
        task_ids =
          from(t in Tasks.Task, where: t.user_id == ^user_id, select: t.id)
          |> Repo.all()

        steps_query =
          Enum.reduce(
            task_ids,
            from(s in Tasks.Step),
            fn query, task_id -> or_where(query, [s], s.task_id == ^task_id) end
          )

        sync_render(conn, params, steps_query)
      end

  This works but when the user adds a new task, existing requests from clients
  won't pick up new tasks until active long-poll requests complete, which means
  that new tasks may not appear in the page until up to 20 seconds later.

  To handle this situation you can make your `sync_render/3` call interruptible
  like so:

      def steps(conn, %{"user_id" => user_id} = params) do
        # queries as before..

        sync_render(conn, params, steps_query, interruptible: true)
      end

  And add an interrupt call in your tasks controller to trigger the interrupt:

      def create(conn, %{"user_id" => user_id, "task" => task_params}) do
        # create the task as before...

        # interrupt all active steps shapes
        Phoenix.Sync.Controller.interrupt_matching(Tasks.Step)

        # return the response...
      end

  Now active long-poll requests will be interrupted and the clients will
  immediately re-try and receive the updated shape data including the new task.

  For more information see `Phoenix.Sync.Controller.interrupt_matching/2`.
  """

  alias Phoenix.Sync.Plug.CORS
  alias Phoenix.Sync.PredefinedShape

  defmacro __using__(opts \\ []) do
    # validate that we're being used in the context of a Plug.Router impl
    Phoenix.Sync.Plug.Utils.env!(__CALLER__)

    quote do
      @plug_assign_opts Phoenix.Sync.Plug.Utils.opts_in_assign!(
                          unquote(opts),
                          __MODULE__,
                          Phoenix.Sync.Controller
                        )

      def sync_render(conn, shape, shape_opts \\ []) do
        case get_in(conn.assigns, [@plug_assign_opts, :phoenix_sync]) do
          %_{} = api ->
            conn =
              conn
              |> Plug.Conn.fetch_query_params()
              |> Plug.Conn.put_private(:phoenix_sync_api, api)

            Phoenix.Sync.Controller.sync_render(conn, conn.params, shape, shape_opts)

          nil ->
            raise RuntimeError,
              message:
                "Please configure your Router opts with [phoenix_sync: Phoenix.Sync.plug_opts()]"
        end
      end
    end
  end

  @doc """
  Return the sync events for the given shape as a `Plug.Conn` response.
  """
  @spec sync_render(
          Plug.Conn.t(),
          Plug.Conn.params(),
          PredefinedShape.shape(),
          PredefinedShape.options()
        ) :: Plug.Conn.t()
  def sync_render(conn, params, shape, shape_opts \\ [])

  def sync_render(%{private: %{phoenix_endpoint: endpoint}} = conn, params, shape, shape_opts) do
    api =
      endpoint.config(:phoenix_sync) ||
        raise RuntimeError,
          message:
            "Please configure your Endpoint with [phoenix_sync: Phoenix.Sync.plug_opts()] in your `c:Application.start/2`"

    sync_render_api(conn, api, params, shape, shape_opts)
  end

  # the Plug.{Router, Builder} version
  def sync_render(%{private: %{phoenix_sync_api: api}} = conn, params, shape, shape_opts) do
    sync_render_api(conn, api, params, shape, shape_opts)
  end

  defp sync_render_api(conn, api, params, shape, shape_opts) do
    {interruptible?, shape, shape_opts} = interruptible_call?(shape, shape_opts, params)

    predefined_shape = PredefinedShape.new!(shape, shape_opts)

    {:ok, shape_api} = Phoenix.Sync.Adapter.PlugApi.predefined_shape(api, predefined_shape)

    if interruptible? do
      interruptible_call(shape_api, predefined_shape, conn, params)
    else
      Phoenix.Sync.Adapter.PlugApi.call(shape_api, conn, params)
    end
    |> CORS.call()
  end

  defp interruptible_call?(shape, shape_opts, params) do
    # support both spellings because I'm even switching between them randomly
    # as I write this feature and the difference is hard to spot
    Enum.reduce([:interruptible, :interruptable], {false, shape, shape_opts}, fn
      opt, {interruptible?, shape, shape_opts} ->
        {shape_interruptible?, shape} = interruptible_call_opts(shape, opt)
        {shape_opts_interruptible?, shape_opts} = interruptible_call_opts(shape_opts, opt)
        {interruptible? || shape_interruptible? || shape_opts_interruptible?, shape, shape_opts}
    end)
    |> then(fn {interruptible?, shape, shape_opts} ->
      {interruptible? && params["live"] == "true", shape, shape_opts}
    end)
  end

  defp interruptible_call_opts(shape_opts, key) when is_list(shape_opts) do
    Keyword.pop(shape_opts, key, false)
  end

  defp interruptible_call_opts(shape_opts, _key), do: {false, shape_opts}

  defp interruptible_call(shape_api, predefined_shape, conn, params) do
    alias Phoenix.Sync.ShapeRequestRegistry
    {:ok, key} = ShapeRequestRegistry.register_shape(predefined_shape)

    try do
      parent = self()

      {:ok, pid} =
        Task.start_link(fn ->
          send(
            parent,
            {:response, self(), Phoenix.Sync.Adapter.PlugApi.call(shape_api, conn, params)}
          )
        end)

      ref = Process.monitor(pid)

      receive do
        {:interrupt_shape, ^key, :server_interrupt} ->
          Process.demonitor(ref, [:flush])
          Process.unlink(pid)
          Process.exit(pid, :kill)
          interruption_response(conn, params)

        {:response, ^pid, conn} ->
          Process.demonitor(ref, [:flush])
          conn

        {:DOWN, ^ref, :process, _pid, reason} ->
          Plug.Conn.send_resp(conn, 500, inspect(reason))
      end
    after
      ShapeRequestRegistry.unregister_shape(key)
    end
  end

  @map_params %{
    "handle" => "electric-handle",
    "offset" => "electric-offset",
    "cursor" => "electric-cursor"
  }

  defp interruption_response(conn, params) do
    headers =
      for {key, value} <- params, header = Map.get(@map_params, key), !is_nil(header), into: [] do
        {header, value}
      end

    headers
    |> Enum.reduce(conn, &Plug.Conn.put_resp_header(&2, elem(&1, 0), elem(&1, 1)))
    |> Plug.Conn.put_resp_header("content-type", "application/json; charset=utf-8")
    |> Plug.Conn.put_resp_header(
      "cache-control",
      "no-cache, no-store, must-revalidate, max-age=0"
    )
    |> Plug.Conn.send_resp(200, "[]")
  end

  @params_types {:or, [:string, :integer, :float, :boolean]}
  @schema NimbleOptions.new!(
            table: [type: :string, required: true],
            namespace: [
              type: {:or, [nil, :string]}
            ],
            where: [
              type: {:or, [nil, :string]}
            ],
            columns: [
              type: {:or, [nil, {:list, :string}]}
            ],
            params: [
              type: {:or, [nil, {:map, :pos_integer, @params_types}, {:list, @params_types}]}
            ]
          )

  @type options() :: [unquote(NimbleOptions.option_typespec(@schema))]

  if Code.ensure_loaded?(Ecto) do
    @type shape() :: options() | Electric.Client.ecto_shape()
  else
    @type shape() :: options()
  end

  @type shape_opts() :: options()

  @doc """
  Breaks all long-polling requests maching the give shape definition.

  The broader the shape definition, the more requests will be interrupted.

  ### Examples

  To interrupt all shapes on the "todos" table:

      Phoenix.Sync.Controller.interrupt_matching("todos")
      Phoenix.Sync.Controller.interrupt_matching(table: "todos")

  or the same using an `Ecto.Schema` module:

      Phoenix.Sync.Controller.interrupt_matching(Todos.Todo)

  all shapes with the given parameterized where clause:

      Phoenix.Sync.Controller.interrupt_matching(table: "todos", where: "user_id = $1")

  or a single shape for the given user:

      Phoenix.Sync.Controller.interrupt_matching(
        from(t in Todos.Todo, where: t.user_id == ^user_id)
      )

  Be careful when mixing `Ecto` query-based shapes with interrupt calls using
  hand-written where clauses.

  The shape

      Phoenix.Sync.Controller.sync_stream(
        conn,
        params,
        from(t in Todos.Todo, where: t.user_id == ^user_id)
      )

  will **not** be interrupted by

      Phoenix.Sync.Controller.interrupt_matching(
        table: "todos",
        where: "user_id = '\#{user_id}'"
      )

  because the where clause matching is *exact string-based*.

  You're better off being too broad with your interrupt calls than too narrow
  as if a shape is unaffected, the clients will experience very little or no
  delay in their sync data.

  Returns the number of interrupted requests.

  ## Supported options

  #{NimbleOptions.docs(@schema)}
  """
  @spec interrupt_matching(shape(), shape_opts()) :: {:ok, pos_integer()}
  def interrupt_matching(shape, shape_opts \\ []) do
    Phoenix.Sync.ShapeRequestRegistry.interrupt_matching(shape, shape_opts)
  end
end
