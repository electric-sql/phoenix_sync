defmodule Phoenix.Sync.LiveViewTest.TodoController do
  use Phoenix.Controller, formats: [:html, :json]

  import Plug.Conn
  import Phoenix.Sync.Controller

  import Ecto.Query, only: [from: 2]

  def all(conn, params) do
    sync_render(conn, params, table: "todos")
  end

  def complete(conn, params) do
    sync_render(conn, params, table: "todos", where: "completed = true")
  end

  def flexible(conn, %{"completed" => completed} = params) do
    sync_render(conn, params, from(t in Support.Todo, where: t.completed == ^completed))
  end

  def module(conn, params) do
    sync_render(conn, params, Support.Todo)
  end

  def changeset(conn, params) do
    sync_render(conn, params, &Support.Todo.changeset/1)
  end

  def complex(conn, params) do
    sync_render(conn, params, &Support.Todo.changeset/1, where: "completed = false")
  end

  def interruptible(conn, params) do
    sync_render(conn, params, fn ->
      &Support.Todo.changeset/1
    end)
  end

  def interruptible_dynamic(conn, params) do
    sync_render(conn, params, fn ->
      shape_params = Agent.get(:interruptible_dynamic_agent, & &1)

      Phoenix.Sync.shape!(
        table: "todos",
        where: "completed = $1",
        params: shape_params
      )
    end)
  end
end
