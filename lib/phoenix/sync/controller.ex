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

  """

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
  @spec sync_render(Plug.Conn.t(), Plug.Conn.params(), Electric.Shapes.Api.shape_opts()) ::
          Plug.Conn.t()
  def sync_render(%{private: %{phoenix_endpoint: endpoint}} = conn, params, shape) do
    api =
      endpoint.config(:phoenix_sync) ||
        raise RuntimeError,
          message:
            "Please configure your Endpoint with [phoenix_sync: Phoenix.Sync.plug_opts()] in your `c:Application.start/2`"

    sync_render_api(conn, api, params, shape)
  end

  # the Plug.{Router, Builder} version
  def sync_render(%{private: %{phoenix_sync_api: api}} = conn, params, shape) do
    sync_render_api(conn, api, params, shape)
  end

  defp sync_render_api(conn, api, params, shape) do
    predefined_shape = Phoenix.Sync.PredefinedShape.new!(shape)

    {:ok, shape_api} = Phoenix.Sync.Adapter.PlugApi.predefined_shape(api, predefined_shape)

    Phoenix.Sync.Adapter.PlugApi.call(shape_api, conn, params)
  end
end
