defmodule Phoenix.Sync.LiveViewTest.Endpoint do
  use Phoenix.Endpoint, otp_app: :phoenix_sync

  @parsers Plug.Parsers.init(
             parsers: [:urlencoded, :multipart, :json],
             pass: ["*/*"],
             json_decoder: Phoenix.json_library()
           )

  socket("/live", Phoenix.LiveView.Socket)

  defoverridable url: 0, script_name: 0, config: 1, config: 2, static_path: 1

  def url(), do: "http://localhost:4004"
  def script_name(), do: []
  def static_path(path), do: "/static" <> path
  def config(:live_view), do: [signing_salt: "112345678212345678312345678412"]
  def config(:secret_key_base), do: String.duplicate("57689", 50)
  def config(:cache_static_manifest_latest), do: Process.get(:cache_static_manifest_latest)
  def config(:otp_app), do: :phoenix_sync
  # def config(:render_errors), do: [view: __MODULE__]
  def config(:render_errors), do: [view: __MODULE__, accepts: ~w(html json)]
  def config(:static_url), do: [path: "/static"]

  def config(which) do
    super(which)
  end

  def config(which, default), do: super(which, default)

  def call(conn, _) do
    %{conn | secret_key_base: config(:secret_key_base)}
    |> Plug.Parsers.call(@parsers)
    |> Plug.Conn.put_private(:phoenix_endpoint, __MODULE__)
    |> Phoenix.Sync.LiveViewTest.Router.call([])
  end

  def render("500.html", conn) do
    Exception.message(conn.reason)
  end
end
