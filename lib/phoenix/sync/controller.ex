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

  See `Phoenix.Sync.shape!/2` for examples of shape definitions

  ## Interruptible requests

  There may be circumstances where shape definitions are dynamic based on, say,
  a database query. In this case you should wrap your shape definitions in a
  function and use `sync_render/3` so that changes to clients' shapes can be.
  immediately picked up by the clients.

  For more information see `Phoenix.Sync.Controller.sync_render/3` and
  `Phoenix.Sync.interrupt/2`.
  """

  alias Phoenix.Sync.Adapter
  alias Phoenix.Sync.Plug.CORS
  alias Phoenix.Sync.PredefinedShape
  alias Phoenix.Sync.ShapeRequestRegistry

  require Logger

  @type shape_option() :: PredefinedShape.option()
  @type shape_options() :: [shape_option()]

  if Code.ensure_loaded?(Ecto) do
    @type shape() :: shape_options() | Electric.Client.ecto_shape()
  else
    @type shape() :: shape_options()
  end

  defmacro __using__(opts \\ []) do
    # validate that we're being used in the context of a Plug.Router impl
    Phoenix.Sync.Plug.Utils.env!(__CALLER__)

    quote do
      @plug_assign_opts Phoenix.Sync.Plug.Utils.opts_in_assign!(
                          unquote(opts),
                          __MODULE__,
                          Phoenix.Sync.Controller
                        )

      def sync_render(conn, shape_fun) when is_function(shape_fun, 0) do
        conn
        |> Phoenix.Sync.Controller.configure_plug_conn!(@plug_assign_opts)
        |> Phoenix.Sync.Controller.sync_render(conn.params, shape_fun)
      end

      def sync_render(conn, shape, shape_opts \\ []) do
        conn
        |> Phoenix.Sync.Controller.configure_plug_conn!(@plug_assign_opts)
        |> Phoenix.Sync.Controller.sync_render(conn.params, shape, shape_opts)
      end
    end
  end

  @doc false
  def configure_plug_conn!(conn, assign_opts) do
    case get_in(conn.assigns, [assign_opts, :phoenix_sync]) do
      %_{} = api ->
        conn
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.put_private(:phoenix_sync_api, api)

      nil ->
        raise RuntimeError,
          message:
            "Please configure your Router opts with [phoenix_sync: Phoenix.Sync.plug_opts()]"
    end
  end

  @doc """
  Return the sync events for the given shape with an interruptible response.

  By passing the shape definition as a function you enable interruptible
  requests. This is useful when the shape definition is dynamic and may change.

  By interrupting the long running requests to the sync API, changes to the
  client's shape can be picked up immediately without waiting for the long-poll
  timeout to expire.

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
        sync_render(conn, params, fn ->
          task_ids =
            from(t in Tasks.Task, where: t.user_id == ^user_id, select: t.id)
            |> Repo.all()

          Enum.reduce(
            task_ids,
            from(s in Tasks.Step),
            fn query, task_id -> or_where(query, [s], s.task_id == ^task_id) end
          )
        end)
      end

  And add an interrupt call in your tasks controller to trigger the interrupt:

      def create(conn, %{"user_id" => user_id, "task" => task_params}) do
        # create the task as before...

        # interrupt all active steps shapes
        Phoenix.Sync.interrupt(Tasks.Step)

        # return the response...
      end

  Now active long-poll requests to the `steps` table will be interrupted and
  re-tried and clients will receive the updated shape data including the new
  task immediately.

  If you want to use keyword-based shapes instead of Ecto queries or add
  options to Ecto shapes, you can use `Phoenix.Sync.shape!/2` in the shape
  definition function:

      sync_render(conn, params, fn ->
        Phoenix.Sync.shape!(query, replica: :full)
      end)

  """
  @spec sync_render(
          Plug.Conn.t(),
          Plug.Conn.params(),
          (-> PredefinedShape.t() | PredefinedShape.shape())
        ) :: Plug.Conn.t()
  def sync_render(conn, params, shape_fun) when is_function(shape_fun, 0) do
    api = configured_api!(conn)

    if interruptible_call?(params) do
      conn
      |> CORS.call()
      |> interruptible_call(api, params, shape_fun)
    else
      predefined_shape = call_shape_fun(shape_fun)
      sync_render_call(conn, api, params, predefined_shape)
    end
  end

  @doc """
  Return the sync events for the given shape as a `Plug.Conn` response.
  """
  @spec sync_render(Plug.Conn.t(), Plug.Conn.params(), shape(), shape_options()) :: Plug.Conn.t()
  def sync_render(conn, params, shape, shape_opts \\ [])

  def sync_render(conn, params, shape, shape_opts) do
    api = configured_api!(conn)
    predefined_shape = PredefinedShape.new!(shape, shape_opts)
    sync_render_call(conn, api, params, predefined_shape)
  end

  # The Phoenix.Controller version
  defp configured_api!(%{private: %{phoenix_endpoint: endpoint}} = _conn) do
    endpoint.config(:phoenix_sync) ||
      raise RuntimeError,
        message:
          "Please configure your Endpoint with [phoenix_sync: Phoenix.Sync.plug_opts()] in your `c:Application.start/2`"
  end

  # the Plug.{Router, Builder} version
  defp configured_api!(%{private: %{phoenix_sync_api: api}} = _conn) do
    api
  end

  defp sync_render_call(conn, api, params, predefined_shape) do
    Phoenix.Sync.Electric.api_predefined_shape(conn, api, predefined_shape, fn conn, shape_api ->
      Phoenix.Sync.Adapter.PlugApi.call(shape_api, CORS.call(conn), params)
    end)
  end

  defp interruptible_call(conn, api, params, shape_fun) do
    predefined_shape = call_shape_fun(shape_fun)

    Phoenix.Sync.Electric.api_predefined_shape(conn, api, predefined_shape, fn conn, shape_api ->
      {:ok, key} = ShapeRequestRegistry.register_shape(predefined_shape)

      try do
        parent = self()
        start_time = now()

        {:ok, pid} =
          Task.start_link(fn ->
            send(parent, {:response, self(), Adapter.PlugApi.call(shape_api, conn, params)})
          end)

        ref = Process.monitor(pid)

        receive do
          {:interrupt_shape, ^key, :server_interrupt} ->
            Process.demonitor(ref, [:flush])
            Process.unlink(pid)
            Process.exit(pid, :kill)
            # immediately retry the same request -- if the shape_fun returns a
            # different shape the client will receive a must-refetch response but
            # if the shape is the same then the request will continue with no
            # interruption.
            #
            # if possible adjust the long poll timeout to account for the time
            # already spent before the interrupt.

            api = reduce_long_poll_timeout(api, start_time)

            interruptible_call(conn, api, params, shape_fun)

          {:response, ^pid, conn} ->
            Process.demonitor(ref, [:flush])

            conn

          {:DOWN, ^ref, :process, _pid, reason} ->
            Plug.Conn.send_resp(conn, 500, inspect(reason))
        end
      after
        ShapeRequestRegistry.unregister_shape(key)
      end
    end)
  end

  defp interruptible_call?(params) do
    params["live"] == "true"
  end

  defp now, do: System.monotonic_time(:millisecond)

  # only calls to the embedded api can have their timeout's adjusted
  defp reduce_long_poll_timeout(%{long_poll_timeout: long_poll_timeout} = api, start_time) do
    timeout = long_poll_timeout - (now() - start_time)

    Logger.debug(fn ->
      ["Restarting interrupted request with timeout: ", to_string(timeout), "ms"]
    end)

    %{api | long_poll_timeout: timeout}
  end

  defp reduce_long_poll_timeout(api_impl, _start_time) do
    api_impl
  end

  defp call_shape_fun(shape_fun) when is_function(shape_fun, 0) do
    case shape_fun.() do
      %PredefinedShape{} = predefined_shape ->
        predefined_shape

      shape_defn ->
        PredefinedShape.new!(shape_defn)
    end
  end
end
