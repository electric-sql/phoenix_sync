defmodule Phoenix.Sync.LiveViewTest.Router do
  use Phoenix.Router

  import Phoenix.LiveView.Router

  pipeline :setup_session do
    plug Plug.Session,
      store: :cookie,
      key: "_live_view_key",
      signing_salt: "/VEDsdfsffMnp5"

    plug :fetch_session
  end

  pipeline :browser do
    plug :setup_session
    plug :accepts, ["html"]
    plug :fetch_live_flash
  end

  scope "/", Phoenix.Sync.LiveViewTest do
    pipe_through [:browser]

    live "/stream", StreamLive
    live "/stream/with-component", StreamLiveWithComponent
    live "/stream/sandbox", StreamSandbox
  end

  scope "/" do
    pipe_through [:browser]

    get "/shape/items", Phoenix.Sync.Plug,
      shape: Electric.Client.shape!("items", where: "visible = true")

    get "/shape/generic", Phoenix.Sync.Plug, []
  end
end
