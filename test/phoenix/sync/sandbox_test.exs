defmodule Phoenix.Sync.SandboxTest do
  use ExUnit.Case, async: true

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)

  alias Support.SandboxRepo, as: Repo
  alias Support.Todo

  setup_all _ctx do
    # start_link_supervised!(Repo)
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
              Todo,
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
  setup do
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)

    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    [db_conn: Repo]
  end

  setup _tags do
    Phoenix.Sync.Test.Sandbox.checkout(Repo) |> dbg
    :ok
  end

  # setup tags do
  #   pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not tags[:async])
  #   on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
  #   :ok
  # end

  setup [
    :with_table,
    :with_data
  ]

  @moduletag table: {
               "todos",
               [
                 "id serial8 not null primary key",
                 "title text",
                 "completed boolean default false"
               ]
             }
  @moduletag data: {"todos", ["title"], [["one"], ["two"], ["three"]]}

  test "1" do
    task1 =
      Task.async(fn ->
        %Todo{} = todo = Repo.get(Todo, 1)

        changeset =
          todo
          |> Ecto.Changeset.change(title: "updated title 1")

        Repo.transaction(fn ->
          Repo.insert!(%Todo{id: 99, title: "wild"})
          Repo.query("select 1", [])
          Repo.update(changeset)
          Repo.delete!(%Todo{id: 99, title: "wild"})
        end)
      end)

    task2 =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query("select 2", [])
          Repo.insert!(%Todo{id: 100, title: "wilder"})
          Repo.delete!(%Todo{id: 100})
        end)
      end)

    Task.await(task1)
    Task.await(task2)
  end

  test "2" do
    task1 =
      Task.async(fn ->
        %Todo{} = todo = Repo.get(Todo, 1)

        changeset =
          todo
          |> Ecto.Changeset.change(title: "updated title 1")

        Repo.transaction(fn ->
          Repo.insert!(%Todo{id: 99, title: "wild"})
          Repo.query("select 1", [])
          Repo.update(changeset)
          Repo.delete!(%Todo{id: 99, title: "wild"})
        end)
      end)

    task2 =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query("select 2", [])
          Repo.insert!(%Todo{id: 100, title: "wilder"})
          Repo.delete!(%Todo{id: 100})
        end)
      end)

    Task.await(task1)
    Task.await(task2)
  end

  test "3" do
    pid =
      spawn_link(fn ->
        receive(do: (:continue -> :ok))
        dbg(linked: self())

        Repo.insert(%Todo{id: 200, title: "linked"})
      end)

    Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), pid)
    send(pid, :continue)

    task1 =
      Task.async(fn ->
        %Todo{} = todo = Repo.get(Todo, 1)

        changeset =
          todo
          |> Ecto.Changeset.change(title: "updated title 1")

        Repo.transaction(fn ->
          Repo.insert!(%Todo{id: 99, title: "wild"})
          Repo.query("select 1", [])
          Repo.update(changeset)
          Repo.delete!(%Todo{id: 99, title: "wild"})
        end)
      end)

    task2 =
      Task.async(fn ->
        %Todo{} = todo = Repo.get(Todo, 2)

        changeset =
          todo
          |> Ecto.Changeset.change(title: "updated title 2")

        Repo.transaction(fn ->
          Repo.query("select 2", [])
          Repo.update(changeset)
          Repo.update(changeset)
          Repo.insert!(%Todo{id: 100, title: "wilder"})
          Repo.delete!(%Todo{id: 100})
        end)
      end)

    Repo.insert!(%Todo{id: 101, title: "trailing"})
    Task.await(task1)
    Task.await(task2)
  end
end
