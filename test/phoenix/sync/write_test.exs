defmodule Phoenix.Sync.WriteTest do
  use ExUnit.Case, async: true

  alias Phoenix.Sync.Write
  alias Ecto.Changeset

  alias Support.Repo

  import Ecto.Query

  def with_repo(_ctx) do
    _pid = start_link_supervised!(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)

    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    # pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not ctx[:async])
    # on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
    [repo: Repo]
  end

  defp changeset_id(changeset), do: Changeset.fetch_field!(changeset, :id)

  def todo_changeset(todo, data, pid) do
    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> tap(&send(pid, {:todo, :changeset, changeset_id(&1)}))
  end

  def todo_changeset(todo, action, data, pid) when action in [:insert, :update, :delete] do
    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> tap(&send(pid, {:todo, :changeset, action, changeset_id(&1)}))
  end

  def delete_changeset(todo, pid) do
    send(pid, {:todo, :delete, todo})
    todo
  end

  def todo_insert_changeset(todo, data, pid) do
    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> Changeset.validate_required([:id, :title, :completed])
    |> tap(&send(pid, {:todo, :insert, changeset_id(&1)}))
  end

  def todo_update_changeset(todo, data, pid) do
    send(pid, {:todo, :update, todo.id, data})

    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> Changeset.validate_required([:id, :title, :completed])
  end

  def todo_delete_changeset(todo, data, pid) do
    send(pid, {:todo, :delete, todo.id})

    todo
    |> Changeset.cast(data, [:id])
    |> Changeset.validate_required([:id])
  end

  def todo_before_insert(multi, changeset, _changes, pid) do
    send(pid, {:todo, :before_insert, changeset_id(changeset)})
    multi
  end

  def todo_before_update(multi, changeset, _changes, pid) do
    send(pid, {:todo, :before_update, changeset_id(changeset)})

    multi
  end

  def todo_before_delete(multi, changeset, _changes, pid) do
    send(pid, {:todo, :before_delete, changeset_id(changeset)})
    multi
  end

  def todo_after_insert(multi, changeset, _changes, pid) do
    send(pid, {:todo, :after_insert, changeset_id(changeset)})
    multi
  end

  def todo_after_update(multi, changeset, _changes, pid) do
    send(pid, {:todo, :after_update, changeset_id(changeset)})
    multi
  end

  def todo_after_delete(multi, changeset, _changes, pid) do
    send(pid, {:todo, :after_delete, changeset_id(changeset)})
    multi
  end

  def todo_get!(%{"id" => id} = _change, pid) do
    send(pid, {:todo, :get, String.to_integer(id)})
    Repo.get_by!(Support.Todo, id: id)
  end

  defmodule TodoNoChangeset do
    use Ecto.Schema

    schema "todos" do
      field :title, :string
      field :completed, :boolean
    end
  end

  # new creates without applying (so doesn't need a repo)
  # apply creates and applies i.e. new() |> Repo.transaction()
  describe "new/2" do
    test "accepts a schema or a list of schema structs", _ctx do
      assert %Write{} = Write.new(Support.Todo)
      # assert %Write{} = Write.new(Support.Todo) |> Write.allow(TodoNoChangeset)
    end

    test "rejects a schema struct with no changeset/2 function", _ctx do
      assert_raise ArgumentError, fn ->
        Write.new(TodoNoChangeset)
      end

      assert %Write{} = Write.new(TodoNoChangeset, &todo_changeset(&1, &2, &3, nil))
      assert %Write{} = Write.new(TodoNoChangeset, changeset: &todo_changeset(&1, &2, &3, nil))
    end

    test "rejects non-schema module" do
      assert_raise ArgumentError, fn ->
        Write.new(__MODULE__)
      end
    end

    test "allows for complete configuration of behaviour", _ctx do
      # how to get pks?
      # function to load original for updates
      pid = self()

      assert %Write{} =
               Write.new(
                 # minimal -- defaults to Support.Todo.changeset/2, gets
                 # table name from Todo.__schema__(:source)
                 # pk from Todo.__schema__(:primary_key)
                 Support.Todo,
                 # if the remote table name does not match the server one
                 # if table is a string, then we match on the table name, ignoring the namespace
                 table: "todos_local",

                 # if the table is a list/tuple, then we match on the full [_, _] relation in the "mutation"
                 # table: ["public", "todos_local"],

                 # defaults to Repo.get!(Todo, <id>)
                 load: &todo_get!(&1, pid),
                 accept: [:insert, :update, :delete],
                 insert: [
                   changeset: &todo_insert_changeset(&1, &2, pid),
                   after: &todo_after_insert(&1, &2, &3, pid),
                   before: &todo_before_insert(&1, &2, &3, pid)
                 ],
                 update: [
                   changeset: &todo_update_changeset(&1, &2, pid),
                   after: &todo_after_update(&1, &2, &3, pid),
                   before: &todo_before_update(&1, &2, &3, pid)
                 ],
                 delete: [
                   changeset: &todo_delete_changeset(&1, &2, pid),
                   after: &todo_after_delete(&1, &2, &3, pid),
                   before: &todo_before_delete(&1, &2, &3, pid)
                 ]
               )

      # new returns an %Write{} struct
      # this new/1 is a validation step
      # apply() returns a txid
    end
  end

  defp with_todos(%{repo: repo}) do
    Repo.query!(
      "create table todos (id int8 not null primary key, title text not null, completed boolean not null default false)",
      []
    )

    repo.insert_all(Support.Todo, [
      [id: 1, title: "First todo", completed: false],
      [id: 2, title: "Second todo", completed: true]
    ])

    :ok
  end

  describe "apply/2" do
    @describetag wip: true

    setup [:with_repo, :with_todos]

    setup do
      pid = self()

      write_config_todo =
        [
          # if the remote table name does not match the server one
          table: "todos_local",
          # defaults to Repo.get_by!(Todo, id: <id>)
          load: &todo_get!(&1, pid),
          accept: [:insert, :update, :delete],
          # instead of per-action changesets, we can just define a single top-level one
          # changeset: fn todo, params -> Changeset.cast(todo, params, [:id, :title, :completed]) end,
          # if we pass a 3-arity changeset, then the second argument is the action
          # changeset: fn todo, action, params when action in [:insert, :update, :delete] -> Changeset.cast(todo, params, [:id, :title, :completed]) end,
          #
          # the same is true for :after, and :before

          insert: [
            changeset: &todo_insert_changeset(&1, &2, pid),
            after: &todo_after_insert(&1, &2, &3, pid),
            before: &todo_before_insert(&1, &2, &3, pid)
          ],
          update: [
            changeset: &todo_update_changeset(&1, &2, pid),
            after: &todo_after_update(&1, &2, &3, pid),
            before: &todo_before_update(&1, &2, &3, pid)
          ],
          delete: [
            changeset: &todo_delete_changeset(&1, &2, pid),
            after: &todo_after_delete(&1, &2, &3, pid),
            before: &todo_before_delete(&1, &2, &3, pid)
          ]
        ]

      write = Write.new(Support.Todo, write_config_todo)

      # Write.new()
      # |> Write.allow(Support.Todo, changeset: &Todo.changeset/2)
      # |> Write.allow(Support.Config, changeset: &Config.changeset/2)
      [write: write, write_config_todo: write_config_todo]
    end

    test "writes valid changes", ctx do
      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "modified" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "modified" => %{"id" => "99", "title" => "Disposable todo", "completed" => "false"}
        },
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "1", "title" => "Changed title", "completed" => "false"},
          "changes" => %{"completed" => "true"}
        },
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "99", "title" => "New todo", "completed" => "false"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "98", "title" => "Working todo", "completed" => "false"},
          "changes" => %{"title" => "Working todo", "completed" => "true"}
        }
      ]

      assert %Ecto.Multi{} = multi = Write.apply(ctx.write, changes)
      assert {:ok, txid, _values} = Write.transaction(multi, Repo)

      assert is_integer(txid)

      # validate that the callbacks are being called

      assert_receive {:todo, :get, 99}
      assert_receive {:todo, :get, 98}
      # we don't call the load function for inserts
      refute_receive {:todo, :get, 98}, 100
      refute_receive {:todo, :get, 99}, 100
      assert_receive {:todo, :get, 2}
      assert_receive {:todo, :get, 1}
      assert_receive {:todo, :get, 1}

      assert_receive {:todo, :insert, 98}
      assert_receive {:todo, :before_insert, 98}
      assert_receive {:todo, :after_insert, 98}

      assert_receive {:todo, :insert, 99}
      assert_receive {:todo, :before_insert, 99}
      assert_receive {:todo, :after_insert, 99}

      assert_receive {:todo, :delete, 2}
      assert_receive {:todo, :before_delete, 2}
      assert_receive {:todo, :after_delete, 2}

      assert_receive {:todo, :update, 1, %{"completed" => "true"}}
      assert_receive {:todo, :before_update, 1}
      assert_receive {:todo, :after_update, 1}

      assert_receive {:todo, :update, 1, %{"title" => "Changed title"}}
      assert_receive {:todo, :before_update, 1}
      assert_receive {:todo, :after_update, 1}

      assert_receive {:todo, :delete, 99}
      assert_receive {:todo, :before_delete, 99}
      assert_receive {:todo, :after_delete, 99}

      assert_receive {:todo, :update, 98, %{"title" => "Working todo", "completed" => "true"}}
      assert_receive {:todo, :before_update, 98}
      assert_receive {:todo, :after_update, 98}

      assert [
               %Support.Todo{id: 1, title: "Changed title", completed: true},
               %Support.Todo{id: 98, title: "Working todo", completed: true}
             ] = ctx.repo.all(from(t in Support.Todo, order_by: t.id))
    end

    test "always validates that pk columns are included in all mutations", ctx do
      changes = [
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        }
      ]

      assert {:error, _, _, _} = ctx.write |> Write.apply(changes) |> Write.transaction(Repo)
    end

    test "allows for a generic changeset/2 for all mutations", _ctx do
      pid = self()

      write =
        Write.new(Support.Todo,
          table: "todos_local",
          load: &todo_get!(&1, pid),
          changeset: &todo_changeset(&1, &2, pid)
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "modified" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        }
      ]

      assert {:ok, _txid, _changes} = write |> Write.apply(changes) |> Write.transaction(Repo)

      assert_receive {:todo, :changeset, 98}
      assert_receive {:todo, :changeset, 2}
      assert_receive {:todo, :changeset, 1}
    end

    test "allows for a generic changeset/3 for all mutations", _ctx do
      pid = self()

      write =
        Write.new(Support.Todo,
          table: "todos_local",
          load: &todo_get!(&1, pid),
          changeset: &todo_changeset(&1, &2, &3, pid)
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "modified" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        }
      ]

      assert {:ok, _txid, _changes} = write |> Write.apply(changes) |> Write.transaction(Repo)

      assert_receive {:todo, :changeset, :insert, 98}
      assert_receive {:todo, :changeset, :delete, 2}
      assert_receive {:todo, :changeset, :update, 1}
    end

    test "rejects updates not in :accept list", _ctx do
      pid = self()

      write =
        Write.new(Support.Todo,
          table: "todos_local",
          load: &todo_get!(&1, pid),
          accept: [:insert, :update],
          changeset: &todo_changeset(&1, &2, &3, pid)
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "modified" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        }
      ]

      assert {:error, _, _, _changes} = write |> Write.apply(changes) |> Write.transaction(Repo)
    end

    test "allows for a generic before/2 and after/2 for all mutations"
    test "allows for a generic before/3 and after/3 for all mutations"
    test "returns an error and doesn't apply txn if any changeset returns an error"
    test "uses custom table: \"..\" to map local tables to pg ones"
    test "uses custom namespaced table: [\"..\", \"..\"] to map local tables to pg ones"
    test "allows for both top-level and per-action callbacks"

    test "allows for 1-arity delete changeset functions", _ctx do
      pid = self()

      write =
        Write.new(Support.Todo,
          table: "todos_local",
          load: &todo_get!(&1, pid),
          delete: [
            changeset: &delete_changeset(&1, pid)
          ]
        )

      changes = [
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        }
      ]

      assert {:ok, _txid, _changes} = write |> Write.apply(changes) |> Write.transaction(Repo)

      assert_receive {:todo, :delete, %Support.Todo{id: 2}}
    end

    test "allows for mfa style callback definitions", _ctx do
      # mfa style allows for generating compile-time write configs
      pid = self()

      write =
        Write.new(Support.Todo,
          table: "todos_local",
          load: {__MODULE__, :todo_get!, [pid]},
          accept: [:insert, :update, :delete],
          insert: [
            changeset: {__MODULE__, :todo_insert_changeset, [pid]},
            after: {__MODULE__, :todo_after_insert, [pid]},
            before: {__MODULE__, :todo_before_insert, [pid]}
          ],
          update: [
            changeset: {__MODULE__, :todo_update_changeset, [pid]},
            after: {__MODULE__, :todo_after_update, [pid]},
            before: {__MODULE__, :todo_before_update, [pid]}
          ],
          delete: [
            changeset: {__MODULE__, :todo_delete_changeset, [pid]},
            after: {__MODULE__, :todo_after_delete, [pid]},
            before: {__MODULE__, :todo_before_delete, [pid]}
          ]
        )

      changes = [
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        }
      ]

      assert {:ok, _txid, _changes} = write |> Write.apply(changes) |> Write.transaction(Repo)
    end
  end
end
