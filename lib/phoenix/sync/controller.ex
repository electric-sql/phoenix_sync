defmodule Phoenix.Sync.Controller do
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
        case get_in(conn.assigns, [@plug_assign_opts, :electric, :api]) do
          %Electric.Shapes.Api{} = api ->
            conn =
              conn
              |> Plug.Conn.fetch_query_params()
              |> Plug.Conn.put_private(:electric_api, api)

            Phoenix.Sync.Controller.sync_render(conn, conn.params, shape)

          _ ->
            raise RuntimeError,
              message:
                "Please configure your Router opts with [electric: Electric.Shapes.Api.plug_opts()]"
        end
      end
    end
  end

  @spec sync_render(Plug.Conn.t(), Plug.Conn.params(), Electric.Shapes.Api.shape_opts()) ::
          Plug.Conn.t()

  def sync_render(%{private: %{phoenix_endpoint: endpoint}} = conn, params, shape) do
    config = endpoint.config(:electric)

    api =
      config[:api] ||
        raise RuntimeError,
          message:
            "Please configure your Router opts with [electric: Electric.Shapes.Api.plug_opts()]"

    sync_render_api(conn, api, params, shape)
  end

  def sync_render(%{private: %{electric_api: api}} = conn, params, shape) do
    sync_render_api(conn, api, params, shape)
  end

  defp sync_render_api(conn, api, params, shape) do
    predefined_shape = Phoenix.Sync.PredefinedShape.new!(shape)

    {:ok, shape_api} = Phoenix.Sync.Adapter.predefined_shape(api, predefined_shape)

    Phoenix.Sync.Adapter.call(shape_api, conn, params)
  end
end
