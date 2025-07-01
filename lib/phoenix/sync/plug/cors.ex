defmodule Phoenix.Sync.Plug.CORS do
  @moduledoc """
  A `Plug` that adds the necessary CORS headers to responses from Electric sync
  endpoints.

  `Phoenix.Sync.Controller.sync_render/3` and `Phoenix.Sync.Router.sync/2`
  already include these headers so there's no need to add this plug to your
  `Phoenix` or `Plug` router. This module is just exposed as a convenience.
  """

  @behaviour Plug

  @electric_headers [
    "electric-cursor",
    "electric-handle",
    "electric-offset",
    "electric-schema",
    "electric-up-to-date",
    "electric-internal-known-error"
  ]

  @expose_headers ["transfer-encoding" | @electric_headers]

  def init(opts) do
    Map.new(opts)
  end

  def call(conn) do
    conn
    |> Plug.Conn.put_resp_header("access-control-allow-origin", origin(conn))
    |> Plug.Conn.put_resp_header(
      "access-control-allow-methods",
      "GET, POST, PUT, DELETE, OPTIONS"
    )
    |> Plug.Conn.put_resp_header(
      "access-control-expose-headers",
      Enum.join(@expose_headers, ", ")
    )
  end

  def call(conn, _opts) do
    call(conn)
  end

  defp origin(conn) do
    case Plug.Conn.get_req_header(conn, "origin") do
      [] -> "*"
      [origin] -> origin
    end
  end

  @doc false
  @spec electric_headers() :: [String.t()]
  def electric_headers, do: @electric_headers
end
