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

      def sync_render(conn, shape) do
        case get_in(conn.assigns, [@plug_assign_opts, :phoenix_sync]) do
          %_{} = api ->
            conn =
              conn
              |> Plug.Conn.fetch_query_params()
              |> Plug.Conn.put_private(:phoenix_sync_api, api)

            Phoenix.Sync.Controller.sync_render(conn, conn.params, shape)

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
    predefined_shape = PredefinedShape.new!(shape, shape_opts)

    {:ok, shape_api} = Phoenix.Sync.Adapter.PlugApi.predefined_shape(api, predefined_shape)

    live? = params["live"] == "true"

    if live? do
      interruptible_call(shape_api, predefined_shape, conn, params)
    else
      Phoenix.Sync.Adapter.PlugApi.call(shape_api, conn, params)
    end
    |> CORS.call()
  end

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
end
