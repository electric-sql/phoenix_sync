defmodule Phoenix.Sync.Electric.ClientAdapter do
  @moduledoc false

  defstruct [:client, :shape_definition]

  defimpl Phoenix.Sync.Adapter.PlugApi do
    alias Electric.Client

    alias Phoenix.Sync.PredefinedShape

    def predefined_shape(sync_client, %PredefinedShape{} = predefined_shape) do
      shape_client = PredefinedShape.client(sync_client.client, predefined_shape)

      {:ok,
       %Phoenix.Sync.Electric.ClientAdapter{
         client: shape_client,
         shape_definition: predefined_shape
       }}
    end

    # this is the server-defined shape route, so we want to only pass on the
    # per-request/stream position params leaving the shape-definition params
    # from the configured client.
    def call(%{shape_definition: %PredefinedShape{} = shape} = sync_client, conn, params) do
      request =
        Client.request(
          sync_client.client,
          method: :get,
          offset: params["offset"],
          shape_handle: params["handle"],
          live: live?(params["live"]),
          next_cursor: params["cursor"]
        )

      fetch_upstream(sync_client, conn, request, shape)
    end

    # this version is the pure client-defined shape version
    def call(sync_client, %{method: method} = conn, params) do
      request =
        Client.request(
          sync_client.client,
          method: normalise_method(method),
          params: params
        )

      fetch_upstream(sync_client, conn, request, nil)
    end

    defp normalise_method(method), do: method |> String.downcase() |> String.to_atom()
    defp live?(live), do: live == "true"

    defp fetch_upstream(sync_client, conn, request, shape) do
      response =
        case Client.Fetch.request(sync_client.client, request) do
          %Client.Fetch.Response{} = response -> response
          {:error, %Client.Fetch.Response{} = response} -> response
        end

      body =
        if response.status in 200..299 do
          Phoenix.Sync.Electric.map_response_body(
            response.body,
            PredefinedShape.transform_fun(shape)
          )
        else
          response.body
        end

      conn
      |> put_headers(response.headers)
      |> Plug.Conn.send_resp(response.status, body)
    end

    defp put_headers(conn, headers) do
      headers
      |> Map.delete("transfer-encoding")
      |> Enum.reduce(conn, fn {header, values}, conn ->
        Enum.reduce(values, conn, fn value, conn ->
          Plug.Conn.put_resp_header(conn, header, value)
        end)
      end)
    end
  end
end
