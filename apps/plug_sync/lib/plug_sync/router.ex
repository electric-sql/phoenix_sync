defmodule PlugSync.Router do
  use Plug.Router, copy_opts_to_assign: :options
  use Phoenix.Sync.Router
  use Phoenix.Sync.Controller

  plug Plug.Logger, log: :debug
  plug :match
  plug :dispatch

  sync "/items-mapped", table: "items", transform: &PlugSync.Router.map_item/1

  get "/items-interruptible" do
    sync_render(conn, fn -> [table: "items"] end)
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  def map_item(item) do
    [
      Map.update!(item, "value", &Map.update!(&1, "name", fn name -> "#{name} mapped #{&1["id"]}" end)),
    ]
  end
end
