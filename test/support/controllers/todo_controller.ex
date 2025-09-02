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

  def transform(conn, params) do
    sync_render(conn, params, Support.Todo, transform: {__MODULE__, :map_todo, ["mapping"]})
  end

  def transform_capture(conn, params) do
    sync_render(conn, params, Support.Todo, transform: &map_todo(&1, "mapping"))
  end

  def transform_interruptible(conn, params) do
    sync_render(conn, params, fn ->
      shape_params = Agent.get(:interruptible_dynamic_agent, & &1)

      Phoenix.Sync.shape!(
        table: "todos",
        where: "completed = $1",
        params: shape_params,
        transform: &map_todo(&1, "mapping")
      )
    end)
  end

  def transform_organization(conn, params) do
    sync_render(conn, params, Support.Organization, transform: Support.Organization)
  end

  def map_todo(%{"headers" => %{"operation" => op}} = msg, route) do
    Map.update!(msg, "value", fn value ->
      Map.put(value, "merged", "#{route}-#{op}-#{value["id"]}-#{value["title"]}")
    end)
  end
end
