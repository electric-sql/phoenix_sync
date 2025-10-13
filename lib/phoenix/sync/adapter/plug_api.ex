defprotocol Phoenix.Sync.Adapter.PlugApi do
  @moduledoc false

  @type response() :: term()

  @spec predefined_shape(t(), Phoenix.Sync.PredefinedShape.t()) :: {:ok, t()} | {:error, term()}
  def predefined_shape(api, shape)

  @spec call(t(), Plug.Conn.t(), Plug.Conn.params()) :: Plug.Conn.t()
  def call(api, conn, params)

  @spec response(t(), Plug.Conn.t(), Plug.Conn.params()) :: response()
  def response(api, conn, params)

  @spec send_response(t(), Plug.Conn.t(), response()) :: Plug.Conn.t()
  def send_response(api, conn, response)
end
