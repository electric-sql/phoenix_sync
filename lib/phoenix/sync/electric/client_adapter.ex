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
      response =
        case Electric.Client.Fetch.request(sync_client.client, request) do
          {:error, response} -> response
          response -> response
        end

      conn
      |> put_headers(response.headers)
      |> Plug.Conn.send_resp(response.status, response.body)
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
