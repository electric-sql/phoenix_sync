defmodule PlugSync.Router do
  use Plug.Router, copy_opts_to_assign: :options
  use Phoenix.Sync.Router

  plug :match
  plug :dispatch

  sync "/items-mapped", table: "items", transform: &PlugSync.Router.map_item/1

  match _ do
    send_resp(conn, 404, "not found")
  end

  def map_item(item) do
    [
      Map.update!(item, "value", &Map.update!(&1, "name", fn name -> "#{name} mapped 1" end)),
      Map.update!(item, "value", &Map.update!(&1, "name", fn name -> "#{name} mapped 2" end))
    ]
  end
end
