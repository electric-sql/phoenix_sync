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
    def call(%{shape_definition: %PredefinedShape{}} = sync_client, conn, params) do
      request =
        Client.request(
          sync_client.client,
          method: :get,
          offset: params["offset"],
          shape_handle: params["handle"],
          live: live?(params["live"]),
          next_cursor: params["cursor"]
        )

      fetch_upstream(sync_client, conn, request)
    end

    # this version is the pure client-defined shape version
    def call(sync_client, %{method: method} = conn, params) do
      request =
        Client.request(
          sync_client.client,
          method: normalise_method(method),
          params: params
        )

      fetch_upstream(sync_client, conn, request)
    end

    defp normalise_method(method), do: method |> String.downcase() |> String.to_atom()
    defp live?(live), do: live == "true"

    defp fetch_upstream(sync_client, conn, request) do
      request = put_request_headers(request, conn)

      response =
        case Client.Fetch.request(sync_client.client, request) do
          %Client.Fetch.Response{} = response -> response
          {:error, %Client.Fetch.Response{} = response} -> response
        end

      conn
      |> put_conn_headers(response.headers)
      |> Plug.Conn.send_resp(response.status, response.body)
    end

    defp put_request_headers(request, conn) do
      conn.req_headers
      |> Enum.into(%{})
      |> Map.merge(request.headers)
      |> then(&Map.put(request, :headers, &1))
    end

    defp put_conn_headers(conn, headers) do
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
