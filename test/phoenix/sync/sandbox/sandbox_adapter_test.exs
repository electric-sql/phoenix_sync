defmodule Phoenix.Sync.Sandbox.SandboxAdapterTest do
  use ExUnit.Case, async: true

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)

  alias Support.SandboxRepo, as: Repo
  alias Support.Todo

  import Ecto.Query, only: [from: 2]

  @moduletag sandbox: true
  @moduletag table: {
               "todos",
               [
                 "id int8 not null primary key generated always as identity",
                 "title text",
                 "completed boolean default false"
               ]
             }

  setup(_ctx) do
    Ecto.Adapters.SQL.Sandbox.mode(Support.SandboxRepo, :manual)

    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
  end

  setup [
    :with_repo_table
  ]

  describe "no active sandbox" do
    # the sandbox adapter should just pass on queries if no sandbox is active
    # otherwise you **have** to start a sandbox for all db tests
    test "insert" do
      Repo.transaction(fn ->
        assert {:ok, %Todo{}} = Repo.insert(%Todo{title: "aberrant"})
      end)
    end

    test "update_all" do
      Repo.transaction(fn ->
        assert {:ok, %Todo{}} = Repo.insert(%Todo{title: "aberrant"})

        from(t in Todo,
          update: [
            set: [completed: true, title: fragment("upper(?)", t.title)]
          ]
        )
        |> Repo.update_all([])
      end)
    end

    test "insert_all" do
      Repo.transaction(fn ->
        todos = [
          %{title: "one"},
          %{title: "two"},
          %{title: "three"}
        ]

        assert {3, _} = Repo.insert_all(Todo, todos)
      end)
    end

    test "delete_all" do
      Repo.transaction(fn ->
        todos = [
          %{title: "one"},
          %{title: "two"},
          %{title: "three"}
        ]

        assert {3, _} = Repo.insert_all(Todo, todos)
        assert {3, _} = Repo.delete_all(Todo)
      end)
    end

    test "delete" do
      Repo.transaction(fn ->
        assert {:ok, %Todo{} = todo} = Repo.insert(%Todo{title: "aberrant"})
        assert {:ok, %Todo{}} = Repo.delete(todo)
      end)
    end

    test "update" do
      Repo.transaction(fn ->
        assert {:ok, %Todo{} = todo} = Repo.insert(%Todo{title: "aberrant"})
        assert {:ok, %Todo{}} = Repo.update(Ecto.Changeset.change(todo, title: "changed"))
      end)
    end
  end

  defp with_repo_table(ctx) do
    case ctx do
      %{table: {name, columns}} ->
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
end
