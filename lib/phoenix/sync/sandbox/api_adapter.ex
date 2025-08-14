if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.APIAdapter do
    @moduledoc false

    defstruct [:shape]

    alias Phoenix.Sync.Adapter.PlugApi

    defimpl Phoenix.Sync.Adapter.PlugApi do
      def predefined_shape(adapter, shape) do
        {:ok, %{adapter | shape: shape}}
      end

      def call(%{shape: nil} = _adapter, conn, params) do
        shape_api = lookup_api!()
        PlugApi.call(shape_api, conn, params)
      end

      def call(%{shape: shape} = _adapter, conn, params) do
        shape_api = lookup_api!()
        {:ok, shape_api} = PlugApi.predefined_shape(shape_api, shape)

        PlugApi.call(shape_api, conn, params)
      end

      defp lookup_api!() do
        Phoenix.Sync.Sandbox.retrieve_api!()
      end
    end
  end
end
