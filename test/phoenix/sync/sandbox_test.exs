defmodule Phoenix.Sync.SandboxTest do
  use ExUnit.Case, async: true

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)
  Code.ensure_loaded!(Support.User)

  alias Support.SandboxRepo, as: Repo
  alias Support.{User, Todo}

  alias Phoenix.Sync.LiveViewTest.Endpoint

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest
  import Phoenix.Component
  import Plug.Conn

  setup_all _ctx do
    # start_link_supervised!(Repo)
    # Phoenix.Sync.Test.Sandbox.Inspector.load_column_info("todos", )

    :ok
  end

  defp with_table(ctx) do
    case ctx do
      %{table: {name, columns}} ->
        schema = Support.DbSetup.schema_name(name)

        sql =
          """
          CREATE TABLE #{Support.DbSetup.inspect_relation(name)} (
          #{Enum.join(columns, ",\n")}
          )
          """

        Repo.query!(sql, [])

        :ok

      _ ->
        :ok
    end

    :ok
  end

  defp with_data(ctx) do
    case Map.get(ctx, :data, nil) do
      {table, columns, values} ->
        placeholders =
          columns |> Enum.with_index(1) |> Enum.map_join(", ", fn {_, n} -> "$#{n}" end)

        Enum.each(values, fn row_values ->
          todo =
            struct(
              User,
              Enum.zip(columns, row_values) |> Enum.map(fn {c, v} -> {String.to_atom(c), v} end)
            )

          Repo.insert(todo)
        end)

        :ok

      nil ->
        :ok
    end
  end

  # need to start a mock client with every sandbox checkout
  # then make sure that any phoenix.sync operation uses that client
  # our repo needs to have every write wrapped so that we can
  # get the active mock client and send it messages equivalent to
  # those changes after the write
  setup(tags) do
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)

    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    :ok
  end

  setup _tags do
    dbg(test: self())
    Phoenix.Sync.Test.Sandbox.checkout(Repo) |> dbg
    :ok
  end

  setup [
    :with_table,
    :with_data
  ]

  @moduletag table: {
               "users",
               [
                 "id uuid not null primary key",
                 "name text",
                 "age int",
                 "visible boolean default true"
               ]
             }
  @moduletag data: {
               "users",
               ["id", "name", "age", "visible"],
               [
                 ["d3e541a2-b90a-4da7-bd6b-2d61e9eb2e1d", "one", 11, true],
                 ["8212cbc2-04ca-44f2-8919-36b13e27034d", "two", 12, true],
                 ["9f4e9e56-a8fa-4880-9f41-dcffa618a2c2", "three", 13, true]
               ]
             }

  @endpoint Endpoint

  setup do
    # {:ok, conn: Plug.Test.init_test_session(build_conn(), %{})}
    {:ok, conn: Phoenix.Sync.Test.init_test_session(build_conn(), Repo, %{})}
  end

  test "live view", %{conn: conn} do
    conn =
      conn
      |> put_private(:test_pid, self())

    dbg(here: 1)

    {:ok, lv, html} = live(conn, "/stream/sandbox")

    conn =
      conn
      |> put_private(:test_pid, self())

    dbg(here: 2)

    {:ok, lv, html} = live(conn, "/stream/sandbox")
  end

  # test "live view 2" do
  #   task1 =
  #     Task.async(fn ->
  #       %Todo{} = todo = Repo.get(Todo, 1)
  #
  #       changeset =
  #         todo
  #         |> Ecto.Changeset.change(title: "updated title 1")
  #
  #       Repo.transaction(fn ->
  #         Repo.insert!(%Todo{id: 99, title: "wild"}) |> dbg
  #         Repo.query("select 1", [])
  #         Repo.update(changeset)
  #         Repo.delete!(%Todo{id: 99, title: "wild"})
  #       end)
  #     end)
  #
  #   task2 =
  #     Task.async(fn ->
  #       Repo.transaction(fn ->
  #         Repo.query("select 2", [])
  #         Repo.insert!(%Todo{id: 100, title: "wilder"})
  #         Repo.delete!(%Todo{id: 100})
  #       end)
  #     end)
  #
  #   Task.await(task1)
  #   Task.await(task2)
  # end
end
