defprotocol Phoenix.Sync.Adapter do
  @moduledoc false

  @spec predefined_shape(t(), Phoenix.Sync.PredefinedShape.t()) :: {:ok, t()} | {:error, term()}
  def predefined_shape(api, shape)

  @spec call(t(), Plug.Conn.t(), Plug.Conn.params()) :: Plug.Conn.t()
  def call(api, conn, params)
end
