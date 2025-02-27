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
  # sub-optimal way of configuring the api since it means api_config/0 is called on every request
  # better way is to call api_config/0 in Application.start/2
  # and configure the endpoint at that point, e.g.:
  #
  #     electric_config = Electric.Application.api_config()
  #     children = [
  #        # ...
  #        {MyApp.Endpoint, electric: electric_config}
  #     ]
  # or in prod, we can just set the config using Electric.Application.api_plug_opts/1
  # in runtime.exs:
  #
  #     config :my_app, MyEndpoint,
  #       electric: Electric.Application.api_plug_opts()
  # FIXME: we should be able to do something like
  #
  #     config :my_app, MyEndpoint,
  #       electric: Electric.Application.api_plug_opts(repo: MyApp.Repo)
  #
  # with the rest of the config filled by defaults.
  #
  # Because Electric.Application.api_plug_opts/0 uses the application
  # config, we need to first configure electric, then call this.
  # Shouldn't we be able to just configure electric in one place?
  # Maybe that's the reason to use the {endpoint, electric: config} version
  # def config(:electric) do
  #   Electric.Application.api_plug_opts()
  # end

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
