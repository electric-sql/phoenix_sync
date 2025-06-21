defmodule Phoenix.Sync.Electric.ClientAdapter do
  @moduledoc false

  defstruct [:client, :shape_definition]

  defimpl Phoenix.Sync.Adapter.PlugApi do
    alias Electric.Client

    alias Phoenix.Sync.PredefinedShape
    alias Phoenix.Sync.ShapeRegistry

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

    # Skip interruptibility for initial requests.
    defp fetch_upstream(%{client: client, shape_definition: nil}, conn, request) do
      response =
        case Client.Fetch.request(client, request) do
          %Client.Fetch.Response{} = response ->
            response

          {:error, %Client.Fetch.Response{} = response} ->
            response
        end

      conn
      |> put_headers(response.headers)
      |> Plug.Conn.send_resp(response.status, response.body)
    end

    defp fetch_upstream(%{shape_definition: shape} = sync_client, conn, request) do
      key = Base.encode64(:crypto.strong_rand_bytes(18))

      ShapeRegistry.register_shape(key, shape)

      response =
        try do
          case interruptible_fetch(sync_client, request, key) do
            %Client.Fetch.Response{} = response ->
              response

            {:error, %Client.Fetch.Response{} = response} ->
              response
          end
        after
          ShapeRegistry.unregister_shape(key)
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

    defp interruptible_fetch(%{client: client}, request, key) do
      task =
        Task.async(fn ->
          Client.Fetch.request(client, request)
        end)

      ref = Process.monitor(task.pid)

      # Wait for an interrupt, or the task to finish.
      receive do
        {:interrupt_shape, ^key, :server_interrupt} ->
          Task.shutdown(task, :brutal_kill)
          Process.demonitor(ref, [:flush])

          interruption_response(request)

        {:DOWN, ^ref, :process, _pid, :normal} ->
          # Task was successful, return the result.
          Task.await(task, 0)

        {:DOWN, ^ref, :process, _pid, reason} ->
          # Task failed, return an error tuple. If not handled,
          # this will result in a 500 response.
          {:error, reason}
      end
    end

    # Construct the response to return with when interrupted.
    # We want this to cause the client to reconnect to the same URL.
    # Ideally we want to avoid putting 500s in the console logs.
    defp interruption_response(%Client.Fetch.Request{params: params}) do
      %Client.Fetch.Response{
        status: 200,
        headers: %{
          # Construct a non-cachable response to make the client reconnect again
          # on the same URL. This avoids guessing the cache config. The subsequent
          # response will replace the response in the shared and browser cache.
          "cache-control" => "no-cache, no-store, must-revalidate, max-age=0",
          "content-type" => "application/json",
          "electric-cursor" => Map.get(params, "cursor", nil),
          "electric-handle" => Map.fetch!(params, "handle"),
          "electric-offset" => Map.fetch!(params, "offset"),
          "expires" => "0",
          "pragma" => "no-cache",
          "sync-interrupted" => "true"
        },
        body: [],
        request_timestamp: DateTime.utc_now()
      }
    end
  end
end
