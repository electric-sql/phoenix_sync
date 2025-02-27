defmodule Phoenix.Sync.LiveViewTest.Router do
  use Phoenix.Router

  import Phoenix.LiveView.Router
  import Phoenix.Sync.Router

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
  end

  scope "/" do
    pipe_through [:browser]

    get "/shape/items", Phoenix.Sync.Plug,
      shape: Electric.Client.shape!("items", where: "visible = true")

    get "/shape/generic", Phoenix.Sync.Plug, []
  end

  scope "/todos", Phoenix.Sync.LiveViewTest do
    pipe_through [:browser]

    get "/all", TodoController, :all
    get "/complete", TodoController, :complete
    get "/flexible", TodoController, :flexible
    get "/module", TodoController, :module
  end

  scope "/sync" do
    # by default we take the table name from the path
    # note that this does not handle weird table names that need quoting
    # or namespaces
    sync "/todos", Support.Todo

    # or we can expliclty specify the table
    sync "/things-to-do", table: "todos"

    # to use a non-standard namespace, we include it
    # so in this case the table is "food"."toeats"
    sync "/toeats", table: "toeats", namespace: "food"

    # or we can expliclty specify the table
    sync "/ideas",
      table: "ideas",
      where: "plausible = true",
      columns: ["id", "title"],
      replica: :full,
      storage: %{compaction: :disabled}

    # support shapes from a query, passed as the 2nd arg
    # #sdf
    sync "/query-where", Support.Todo, where: "completed = false"

    # or as query: ...
    sync "/query-bare", Support.Todo

    # query version also accepts shape config
    sync "/query-config", Support.Todo, replica: :full
    sync "/query-config2", Support.Todo, replica: :full, storage: %{compaction: :disabled}
  end

  scope "/api" do
    pipe_through [:browser]

    forward "/", Phoenix.Sync.Plug.Shapes
  end
end
