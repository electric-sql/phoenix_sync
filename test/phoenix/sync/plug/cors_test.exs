defmodule Phoenix.Sync.Plug.CorsHeadersTest do
  use ExUnit.Case, async: true

  alias Phoenix.Sync.Plug.CORS

  import Plug.Test

  test "electric headers are up-to-date with current electric" do
    # in test we always have electric as a dependency so we can test
    # that our vendored headers are up-to-date
    assert CORS.electric_headers() == Electric.Shapes.Api.Response.electric_headers()
  end

  test "adds access-control-expose-headers header to response" do
    resp =
      conn(:get, "/sync/bananas")
      |> CORS.call(%{})

    [expose] = Plug.Conn.get_resp_header(resp, "access-control-expose-headers")

    for header <- CORS.electric_headers() do
      assert String.contains?(expose, header)
    end

    assert String.contains?(expose, "transfer-encoding")
  end

  test "adds access-control-allow-origin header to response" do
    resp =
      conn(:get, "/v1/shape")
      |> CORS.call(%{})

    ["*"] = Plug.Conn.get_resp_header(resp, "access-control-allow-origin")

    resp =
      conn(:get, "/large/noise")
      |> Plug.Conn.put_req_header("origin", "https://example.com")
      |> CORS.call(%{})

    ["https://example.com"] = Plug.Conn.get_resp_header(resp, "access-control-allow-origin")
  end

  test "adds access-control-allow-methods header to response" do
    resp =
      conn(:get, "/my-shape")
      |> CORS.call(%{})

    ["GET, POST, PUT, DELETE, OPTIONS"] =
      Plug.Conn.get_resp_header(resp, "access-control-allow-methods")
  end
end
