defprotocol Phoenix.Sync.Adapter do
  @moduledoc false

  @spec predefined_shape(t(), Phoenix.Sync.PredefinedShape.t()) :: {:ok, t()} | {:error, term()}
  def predefined_shape(api, shape)

  @spec call(t(), Plug.Conn.t(), Plug.Conn.params()) :: Plug.Conn.t()
  def call(api, conn, params)
end

if Code.ensure_loaded?(Electric.Shapes.Api) do
  defimpl Phoenix.Sync.Adapter, for: Electric.Shapes.Api do
    alias Electric.Shapes

    alias Phoenix.Sync.PredefinedShape

    def predefined_shape(api, %PredefinedShape{} = shape) do
      Shapes.Api.predefined_shape(api, PredefinedShape.to_api_params(shape))
    end

    def call(api, %{method: "GET"} = conn, params) do
      case Shapes.Api.validate(api, params) do
        {:ok, request} ->
          conn
          |> Plug.Conn.assign(:request, request)
          |> Shapes.Api.serve_shape_log(request)

        {:error, response} ->
          conn
          |> Shapes.Api.Response.send(response)
          |> Plug.Conn.halt()
      end
    end

    def call(api, %{method: "DELETE"} = conn, params) do
      case Shapes.Api.validate_for_delete(api, params) do
        {:ok, request} ->
          conn
          |> Plug.Conn.assign(:request, request)
          |> Shapes.Api.delete_shape(request)

        {:error, response} ->
          conn
          |> Shapes.Api.Response.send(response)
          |> Plug.Conn.halt()
      end
    end

    def call(_api, %{method: "OPTIONS"} = conn, _params) do
      Shapes.Api.options(conn)
    end
  end
end
