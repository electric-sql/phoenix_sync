defmodule Phoenix.Sync.WriterTest do
  use ExUnit.Case, async: true

  alias Phoenix.Sync.Writer
  alias Ecto.Changeset

  alias Support.Repo

  import Ecto.Query

  def with_repo(_ctx) do
    _pid = start_link_supervised!(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)

    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    [repo: Repo]
  end

  defp changeset_id(changeset), do: Changeset.fetch_field!(changeset, :id)

  defp notify({pid, ref}, msg) when is_pid(pid) do
    notify(pid, {ref, msg})
  end

  defp notify(pid, msg) when is_pid(pid) do
    send(pid, msg)
  end

  def todo_changeset(todo, :delete, _data, pid) do
    notify(pid, {:todo, :changeset, :delete, todo.id})
    todo
  end

  def todo_changeset(todo, data, action, pid) when action in [:insert, :update, :delete] do
    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> tap(&notify(pid, {:todo, :changeset, action, changeset_id(&1)}))
  end

  def delete_changeset(todo, pid) do
    notify(pid, {:todo, :delete, todo})
    todo
  end

  def todo_insert_changeset(todo, data, pid) do
    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> Changeset.validate_required([:id, :title, :completed])
    |> tap(&notify(pid, {:todo, :insert, changeset_id(&1)}))
  end

  def todo_update_changeset(todo, data, pid) do
    notify(pid, {:todo, :update, todo.id, data})

    todo
    |> Changeset.cast(data, [:id, :title, :completed])
    |> Changeset.validate_required([:id, :title, :completed])
  end

  def todo_delete_changeset(todo, data, pid) do
    notify(pid, {:todo, :delete, todo.id})

    todo
    |> Changeset.cast(data, [:id])
    |> Changeset.validate_required([:id])
  end

  def todo_pre_apply_insert(multi, changeset, _changes, pid) do
    notify(pid, {:todo, :pre_apply_insert, changeset_id(changeset)})
    multi
  end

  def todo_pre_apply_update(multi, changeset, _changes, pid) do
    notify(pid, {:todo, :pre_apply_update, changeset_id(changeset)})

    multi
  end

  def todo_pre_apply_delete(multi, changeset, _changes, pid) do
    notify(pid, {:todo, :pre_apply_delete, changeset_id(changeset)})
    multi
  end

  def todo_post_apply_insert(multi, changeset, _changes, pid) do
    notify(pid, {:todo, :post_apply_insert, changeset_id(changeset)})
    multi
  end

  def todo_post_apply_update(multi, changeset, _changes, pid) do
    notify(pid, {:todo, :post_apply_update, changeset_id(changeset)})
    multi
  end

  def todo_post_apply_delete(multi, changeset, _changes, pid) do
    notify(pid, {:todo, :post_apply_delete, changeset_id(changeset)})
    multi
  end

  def todo_get(%{"id" => id} = _change, pid) do
    notify(pid, {:todo, :get, String.to_integer(id)})
    Repo.get_by(Support.Todo, id: id)
  end

  def todo_check(operation, pid) do
    notify(pid, {:todo, :check, operation})
    :ok
  end

  def writer do
    Writer.new()
  end

  def ingest(writer, changes, opts \\ [format: Writer.Format.TanstackDB]) do
    Writer.ingest(writer, changes, opts)
  end

  def todo_get_tuple(data, pid) do
    case todo_get(data, pid) do
      nil ->
        {:error, "custom error message"}

      %_{} = todo ->
        {:ok, todo}
    end
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
  describe "allow/2" do
    test "accepts a schema and changeset fun", _ctx do
      assert %Writer{} =
               Writer.allow(writer(), TodoNoChangeset, validate: &todo_changeset(&1, &2, &3, nil))

      assert %Writer{} =
               Writer.allow(writer(), TodoNoChangeset, validate: &todo_changeset(&1, &2, &3, nil))
    end

    test "rejects non-schema module" do
      assert_raise ArgumentError, fn ->
        Writer.allow(writer(), __MODULE__)
      end
    end

    test "allows for complete configuration of behaviour", _ctx do
      pid = self()

      assert %Writer{} =
               Writer.allow(
                 writer(),
                 Support.Todo,
                 table: "todos_local",
                 # defaults to Repo.get!(Todo, <id>)
                 load: &todo_get(&1, pid),
                 accept: [:insert, :update, :delete],
                 check: &todo_check(&1, pid),
                 insert: [
                   validate: &todo_insert_changeset(&1, &2, pid),
                   post_apply: &todo_post_apply_insert(&1, &2, &3, pid),
                   pre_apply: &todo_pre_apply_insert(&1, &2, &3, pid)
                 ],
                 update: [
                   validate: &todo_update_changeset(&1, &2, pid),
                   post_apply: &todo_post_apply_update(&1, &2, &3, pid),
                   pre_apply: &todo_pre_apply_update(&1, &2, &3, pid)
                 ],
                 delete: [
                   validate: &todo_delete_changeset(&1, &2, pid),
                   post_apply: &todo_post_apply_delete(&1, &2, &3, pid),
                   pre_apply: &todo_pre_apply_delete(&1, &2, &3, pid)
                 ]
               )
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

  describe "transaction/2" do
    setup [:with_repo, :with_todos]

    setup do
      pid = self()

      writer_config_todo =
        [
          table: "todos_local",
          load: &todo_get(&1, pid),
          accept: [:insert, :update, :delete],
          check: &todo_check(&1, pid),
          insert: [
            validate: &todo_insert_changeset(&1, &2, pid),
            post_apply: &todo_post_apply_insert(&1, &2, &3, pid),
            pre_apply: &todo_pre_apply_insert(&1, &2, &3, pid)
          ],
          update: [
            validate: &todo_update_changeset(&1, &2, pid),
            post_apply: &todo_post_apply_update(&1, &2, &3, pid),
            pre_apply: &todo_pre_apply_update(&1, &2, &3, pid)
          ],
          delete: [
            validate: &todo_delete_changeset(&1, &2, pid),
            post_apply: &todo_post_apply_delete(&1, &2, &3, pid),
            pre_apply: &todo_pre_apply_delete(&1, &2, &3, pid)
          ]
        ]

      writer = Writer.allow(writer(), Support.Todo, writer_config_todo)

      [writer: writer, writer_config_todo: writer_config_todo]
    end

    test "has sensible defaults for load and changeset functions" do
      writer = writer() |> Writer.allow(Support.Todo)

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "modified" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        }
      ]

      assert {:ok, _txid, _values} = writer |> ingest(changes) |> Writer.transaction(Repo)
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

      writer = ingest(ctx.writer, changes)

      assert %Ecto.Multi{} = multi = Writer.to_multi(writer)
      assert {:ok, txid, _values} = Writer.transaction(multi, Repo)

      assert is_integer(txid)

      # validate that the callbacks are being called

      # assert_receive {:todo, :get, 99}
      # assert_receive {:todo, :get, 98}
      # we don't call the load function for inserts
      refute_receive {:todo, :get, 98}, 10
      refute_receive {:todo, :get, 99}, 10
      assert_receive {:todo, :get, 2}
      assert_receive {:todo, :get, 1}

      assert_receive {:todo, :insert, 98}
      assert_receive {:todo, :pre_apply_insert, 98}
      assert_receive {:todo, :post_apply_insert, 98}

      assert_receive {:todo, :insert, 99}
      assert_receive {:todo, :pre_apply_insert, 99}
      assert_receive {:todo, :post_apply_insert, 99}

      assert_receive {:todo, :delete, 2}
      assert_receive {:todo, :pre_apply_delete, 2}
      assert_receive {:todo, :post_apply_delete, 2}

      assert_receive {:todo, :update, 1, %{"completed" => "true"}}
      assert_receive {:todo, :pre_apply_update, 1}
      assert_receive {:todo, :post_apply_update, 1}

      assert_receive {:todo, :update, 1, %{"title" => "Changed title"}}
      assert_receive {:todo, :pre_apply_update, 1}
      assert_receive {:todo, :post_apply_update, 1}

      assert_receive {:todo, :delete, 99}
      assert_receive {:todo, :pre_apply_delete, 99}
      assert_receive {:todo, :post_apply_delete, 99}

      assert_receive {:todo, :update, 98, %{"title" => "Working todo", "completed" => "true"}}
      assert_receive {:todo, :pre_apply_update, 98}
      assert_receive {:todo, :post_apply_update, 98}

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

      assert {:error, _, _, _} = ctx.writer |> ingest(changes) |> Writer.transaction(Repo)
    end

    test "allows for a generic changeset/3 for all mutations", _ctx do
      pid = self()

      writer =
        Writer.allow(writer(), Support.Todo,
          table: "todos_local",
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, pid)
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

      assert {:ok, _txid, _changes} = writer |> ingest(changes) |> Writer.transaction(Repo)

      assert_receive {:todo, :changeset, :insert, 98}
      assert_receive {:todo, :changeset, :delete, 2}
      assert_receive {:todo, :changeset, :update, 1}
    end

    test "returns an error if original record is not found" do
      pid = self()

      writer =
        Writer.allow(writer(), Support.Todo,
          table: "todos_local",
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, pid)
        )

      changes = [
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "111111", "title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        }
      ]

      assert {:error, {:__phoenix_sync__, :changeset, 0}, %Writer.Error{}, _changes} =
               writer |> ingest(changes) |> Writer.transaction(Repo)
    end

    test "rejects updates not in :accept list", _ctx do
      pid = self()

      writer =
        Writer.allow(writer(), Support.Todo,
          table: "todos_local",
          load: &todo_get(&1, pid),
          accept: [:insert, :update],
          validate: &todo_changeset(&1, &2, &3, pid)
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

      assert {:error, :check, %Writer.Error{}, _changes} =
               writer |> ingest(changes) |> Writer.transaction(Repo)
    end

    test "rejects any txn that fails the check test" do
      pid = self()

      writer =
        Writer.allow(writer(), Support.Todo,
          table: "todos_local",
          load: &todo_get(&1, pid),
          check: fn
            %{operation: :delete} -> {:error, "no deletes!"}
            _op -> :ok
          end,
          validate: &todo_changeset(&1, &2, &3, pid)
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

      assert {:error, :check, %Writer.Error{message: "no deletes!"}, _changes} =
               writer |> ingest(changes) |> Writer.transaction(Repo)
    end

    test "supports accepting writes on multiple tables", _ctx do
      pid = self()
      todo1_ref = make_ref()
      todo2_ref = make_ref()

      writer =
        writer()
        |> Writer.allow(Support.Todo,
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, {pid, todo1_ref}),
          insert: [post_apply: &todo_post_apply_insert(&1, &2, &3, {pid, todo1_ref})]
        )
        |> Writer.allow(Support.Todo,
          table: "todos_2",
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, {pid, todo2_ref}),
          insert: [post_apply: &todo_post_apply_insert(&1, &2, &3, {pid, todo2_ref})]
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "modified" => %{"id" => "98", "title" => "New todo1", "completed" => "false"}
        },
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos_2"]},
          "modified" => %{"id" => "99", "title" => "New todo2", "completed" => "false"}
        }
      ]

      assert {:ok, _txid, _changes} =
               writer |> ingest(changes) |> Writer.transaction(Repo)

      assert_receive {^todo1_ref, {:todo, :changeset, :insert, 98}}
      assert_receive {^todo1_ref, {:todo, :post_apply_insert, 98}}

      assert_receive {^todo2_ref, {:todo, :changeset, :insert, 99}}
      assert_receive {^todo2_ref, {:todo, :post_apply_insert, 99}}
    end

    test "is intelligent about mapping client tables to server", _ctx do
      pid = self()

      writer =
        writer()
        |> Writer.allow(Support.Todo,
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, pid),
          insert: [post_apply: &todo_post_apply_insert(&1, &2, &3, pid)]
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "modified" => %{"id" => "98", "title" => "New todo1", "completed" => "false"}
        },
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["client", "todos"]},
          "modified" => %{"id" => "99", "title" => "New todo2", "completed" => "false"}
        }
      ]

      assert {:ok, _txid, _changes} = writer |> ingest(changes) |> Writer.transaction(Repo)

      assert_receive {:todo, :changeset, :insert, 98}
      assert_receive {:todo, :post_apply_insert, 98}

      assert_receive {:todo, :changeset, :insert, 99}
      assert_receive {:todo, :post_apply_insert, 99}
    end

    test "only matches full relation if configured", _ctx do
      pid = self()

      writer =
        writer()
        |> Writer.allow(Support.Todo,
          table: ["public", "todos"],
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, pid),
          insert: [post_apply: &todo_post_apply_insert(&1, &2, &3, pid)]
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "modified" => %{"id" => "98", "title" => "New todo1", "completed" => "false"}
        },
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["client", "todos"]},
          "modified" => %{"id" => "99", "title" => "New todo2", "completed" => "false"}
        }
      ]

      # we have specified allow/2 with a fully qualified table so only one of the
      # inserts matches
      assert {:error, :check, %Writer.Error{}, _changes} =
               writer |> ingest(changes) |> Writer.transaction(Repo)
    end

    test "allows for 1-arity delete changeset functions", _ctx do
      pid = self()

      writer =
        Writer.allow(writer(), Support.Todo,
          table: "todos_local",
          load: &todo_get(&1, pid),
          delete: [
            validate: &delete_changeset(&1, pid)
          ]
        )

      changes = [
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos_local"]},
          "original" => %{"id" => "2"}
        }
      ]

      assert {:ok, _txid, _changes} = writer |> ingest(changes) |> Writer.transaction(Repo)

      assert_receive {:todo, :delete, %Support.Todo{id: 2}}
    end

    test "allows for a generic pre_apply/3 and post_apply/3 for all mutations" do
      pid = self()

      writer =
        Writer.allow(writer(), Support.Todo,
          table: "todos_local",
          load: &todo_get(&1, pid),
          pre_apply: fn multi, changeset, ctx ->
            send(pid, {:pre_apply, ctx.operation.operation, changeset_id(changeset)})
            multi
          end,
          post_apply: fn multi, changeset, ctx ->
            send(pid, {:post_apply, ctx.operation.operation, changeset_id(changeset)})
            multi
          end
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

      assert {:ok, _txid, _changes} = writer |> ingest(changes) |> Writer.transaction(Repo)

      assert_receive {:pre_apply, :insert, 98}
      assert_receive {:pre_apply, :delete, 2}
      assert_receive {:pre_apply, :update, 1}
      assert_receive {:post_apply, :insert, 98}
      assert_receive {:post_apply, :delete, 2}
      assert_receive {:post_apply, :update, 1}
    end

    test "supports custom mutation message format", ctx do
      pid = self()

      changes = [
        %{
          "perform" => "insert",
          "relation" => ["public", "todos"],
          "updates" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "perform" => "delete",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "2"}
        },
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "updates" => %{"title" => "Changed title"}
        }
      ]

      assert {:ok, _txid, _changes} =
               Writer.new()
               |> Writer.allow(Support.Todo, load: &todo_get(&1, pid))
               |> Writer.ingest(changes, parser: &parse_transaction/1)
               |> Writer.transaction(Repo)

      assert [
               %Support.Todo{id: 1, title: "Changed title", completed: false},
               %Support.Todo{id: 98, title: "New todo", completed: false}
             ] = ctx.repo.all(from(t in Support.Todo, order_by: t.id))
    end

    test "supports custom mutation message format via mfa", ctx do
      pid = self()

      changes = [
        %{
          "perform" => "insert",
          "relation" => ["public", "todos"],
          "updates" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "perform" => "delete",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "2"}
        },
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "updates" => %{"title" => "Changed title"}
        }
      ]

      assert {:ok, _txid, _changes} =
               Writer.new()
               |> Writer.allow(Support.Todo, load: &todo_get(&1, pid))
               |> Writer.ingest(changes, parser: {__MODULE__, :parse_transaction, []})
               |> Writer.transaction(Repo)

      assert [
               %Support.Todo{id: 1, title: "Changed title", completed: false},
               %Support.Todo{id: 98, title: "New todo", completed: false}
             ] = ctx.repo.all(from(t in Support.Todo, order_by: t.id))
    end

    test "uses data in the txn if it exists" do
      # if an update applies to a previously inserted value
      # then rather than use the load fun and retrieve the value
      # we can re-use the value in the multi change data
      pid = self()

      changes = [
        %{
          "perform" => "insert",
          "relation" => ["public", "todos"],
          "updates" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "98"},
          "updates" => %{"title" => "Changed title"}
        },
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "98"},
          "updates" => %{"title" => "Changed again", "completed" => true}
        }
      ]

      assert {:ok, _txid, _changes} =
               Writer.new()
               |> Writer.allow(Support.Todo, load: &todo_get(&1, pid))
               |> Writer.ingest(changes, parser: {__MODULE__, :parse_transaction, []})
               |> Writer.transaction(Repo)

      refute_receive {:todo, :get, 98}, 50

      ## if we delete in the txn then we know it doesn't exist

      changes = [
        %{
          "perform" => "insert",
          "relation" => ["public", "todos"],
          "updates" => %{"id" => "99", "title" => "New todo", "completed" => "false"}
        },
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "99"},
          "updates" => %{"title" => "Changed title"}
        },
        %{
          "perform" => "delete",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "99"}
        },
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "99"},
          "updates" => %{"title" => "Changed title", "completed" => true}
        }
      ]

      assert {:error, _txid, _, _changes} =
               Writer.new()
               |> Writer.allow(Support.Todo, load: &todo_get(&1, pid))
               |> Writer.ingest(changes, parser: {__MODULE__, :parse_transaction, []})
               |> Writer.transaction(Repo)
    end

    test "allows for custom errors from load fun", _ctx do
      pid = self()

      changes1 = [
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "updates" => %{"title" => "Changed title"}
        }
      ]

      assert {:ok, _txid, _changes} =
               Writer.new()
               |> Writer.allow(Support.Todo, load: &todo_get_tuple(&1, pid))
               |> Writer.ingest(changes1, parser: {__MODULE__, :parse_transaction, []})
               |> Writer.transaction(Repo)

      changes2 = [
        %{
          "perform" => "update",
          "relation" => ["public", "todos"],
          "value" => %{"id" => "1001", "title" => "First todo", "completed" => "false"},
          "updates" => %{"title" => "Changed title"}
        }
      ]

      assert {:error, _, %Writer.Error{message: "custom error message"}, _} =
               Writer.new()
               |> Writer.allow(Support.Todo, load: &todo_get_tuple(&1, pid))
               |> Writer.ingest(changes2, parser: {__MODULE__, :parse_transaction, []})
               |> Writer.transaction(Repo)
    end

    test "before_all", _ctx do
      pid = self()
      ref = make_ref()
      counter = :atomics.new(1, signed: false)

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "modified" => %{"id" => "98", "title" => "New todo", "completed" => "false"}
        },
        %{
          "type" => "delete",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "original" => %{"id" => "2"}
        },
        %{
          "type" => "update",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "original" => %{"id" => "1", "title" => "First todo", "completed" => "false"},
          "changes" => %{"title" => "Changed title"}
        }
      ]

      assert {:ok, _txid, %{before_all_todo: ^ref}} =
               writer()
               |> Writer.allow(Support.Todo,
                 load: &todo_get(&1, pid),
                 before_all: fn multi ->
                   :atomics.add(counter, 1, 1)
                   Ecto.Multi.put(multi, :before_all_todo, ref)
                 end
               )
               |> Writer.apply(changes, Repo, format: Writer.Format.TanstackDB)

      assert 1 == :atomics.get(counter, 1)
    end
  end

  describe "txid/1" do
    setup [:with_repo, :with_todos]

    test "returns the txid", _ctx do
      pid = self()

      writer =
        writer()
        |> Writer.allow(Support.Todo,
          load: &todo_get(&1, pid),
          validate: &todo_changeset(&1, &2, &3, pid),
          insert: [post_apply: &todo_post_apply_insert(&1, &2, &3, pid)]
        )

      changes = [
        %{
          "type" => "insert",
          "syncMetadata" => %{"relation" => ["public", "todos"]},
          "modified" => %{"id" => "98", "title" => "New todo1", "completed" => "false"}
        }
      ]

      assert {:ok, changes} = writer |> ingest(changes) |> Writer.to_multi() |> Repo.transaction()

      assert {:ok, txid} = Writer.txid(changes)

      assert is_integer(txid)

      assert txid == Writer.txid!(changes)
    end
  end

  describe "transact/3" do
    setup [:with_repo, :with_todos]

    test "supports any unparsed mutation data" do
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

      parent = self()

      assert {:ok, txid} =
               Writer.transact(
                 changes,
                 Repo,
                 fn
                   %Writer.Operation{
                     operation: op,
                     relation: relation,
                     data: data,
                     changes: changes
                   } ->
                     send(parent, {op, relation, data, changes})
                     :ok
                 end,
                 format: Writer.Format.TanstackDB
               )

      assert is_integer(txid)
      assert_receive {:insert, ["public", "todos_local"], %{}, %{"id" => "98"}}
      assert_receive {:insert, ["public", "todos_local"], %{}, %{"id" => "99"}}
      assert_receive {:delete, ["public", "todos_local"], %{"id" => "2"}, %{}}
      assert_receive {:update, ["public", "todos_local"], %{"id" => "1"}, %{"title" => _}}
      assert_receive {:update, ["public", "todos_local"], %{"id" => "1"}, %{"completed" => _}}
      assert_receive {:delete, ["public", "todos_local"], %{"id" => "99"}, %{}}

      assert_receive {:update, ["public", "todos_local"], %{"id" => "98"},
                      %{"title" => _, "completed" => _}}
    end

    test "parse errors return an error tuple" do
      assert {:error, "no"} =
               Writer.transact([], Repo, fn _ -> :ok end, parser: fn _ -> {:error, "no"} end)
    end

    test "changes are applied" do
      changes = [
        {:insert, %Support.Todo{id: 99, title: "New todo 99"}},
        {:insert, %Support.Todo{id: 100, title: "New todo 100"}}
      ]

      assert {:ok, txid} =
               Writer.transact(
                 changes,
                 Repo,
                 fn
                   {:insert, todo} ->
                     Repo.insert(todo)
                 end,
                 parser: fn ops -> {:ok, %Writer.Transaction{operations: ops}} end
               )

      assert [
               %Support.Todo{id: 1, title: _, completed: false},
               %Support.Todo{id: 2, title: _, completed: true},
               %Support.Todo{id: 99, title: _, completed: false},
               %Support.Todo{id: 100, title: _, completed: false}
             ] = Repo.all(from(t in Support.Todo, order_by: t.id))

      assert is_integer(txid)
    end

    test ":error tuples rollback the transaction" do
      changes = [
        {:insert, %Support.Todo{id: 99, title: "New todo"}},
        {:error, "reject"}
      ]

      assert {:error, "reject"} =
               Writer.transact(
                 changes,
                 Repo,
                 fn
                   {:insert, todo} ->
                     Repo.insert(todo)

                   {:error, _} = error ->
                     error
                 end,
                 parser: fn ops -> {:ok, %Writer.Transaction{operations: ops}} end
               )

      assert [
               %Support.Todo{id: 1, title: _, completed: false},
               %Support.Todo{id: 2, title: _, completed: true}
             ] = Repo.all(from(t in Support.Todo, order_by: t.id))
    end
  end

  def parse_transaction(m) when is_list(m) do
    with {:ok, operations} <-
           Writer.Transaction.parse_operations(m, fn op ->
             Writer.Operation.new(op["perform"], op["relation"], op["value"], op["updates"])
           end) do
      Writer.Transaction.new(operations)
    end
  end
end
