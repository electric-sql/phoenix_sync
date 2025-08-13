if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
  defmodule Phoenix.Sync.Sandbox.Fetch do
    @moduledoc false

    alias Electric.Client
    alias Electric.Client.Fetch

    require Logger

    @callback request(Client.t(), Fetch.Request.t(), opts :: Keyword.t()) ::
                Fetch.Response.t() | {:error, Fetch.Response.t() | term()}

    @behaviour Electric.Client.Fetch.Pool

    def name(stack_id) do
      Phoenix.Sync.Sandbox.name({__MODULE__, stack_id})
    end

    @impl Electric.Client.Fetch.Pool
    def request(%Client{} = client, %Fetch.Request{} = request, opts) do
      {:ok, stack_id} = Keyword.fetch(opts, :stack_id)

      request_id = request_id(client, request, stack_id)

      # The monitor process is unique to the request and launches the actual
      # request as a linked process.
      #
      # This coalesces requests, so no matter how many simultaneous
      # clients we have, we only ever make one request to the backend.
      {:ok, monitor_pid} = start_monitor(stack_id, request_id, request, client)

      try do
        ref = Fetch.Monitor.register(monitor_pid, self())

        Fetch.Monitor.wait(ref)
      catch
        :exit, {reason, _} ->
          Logger.debug(fn ->
            "Request process ended with reason #{inspect(reason)} before we could register. Re-attempting."
          end)

          request(client, request, opts)
      end
    end

    defp start_monitor(stack_id, request_id, request, client) do
      DynamicSupervisor.start_child(
        name(stack_id),
        {Electric.Client.Fetch.Monitor, {request_id, request, client}}
      )
      |> return_existing()
    end

    defp return_existing({:ok, pid}), do: {:ok, pid}
    defp return_existing({:error, {:already_started, pid}}), do: {:ok, pid}
    defp return_existing(error), do: error

    defp request_id(%Client{fetch: {fetch_impl, _}}, %Fetch.Request{} = request, stack_id) do
      {
        fetch_impl,
        stack_id,
        URI.to_string(request.endpoint),
        request.headers,
        Fetch.Request.params(request)
      }
    end
  end
end
