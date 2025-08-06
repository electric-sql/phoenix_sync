defmodule Phoenix.Sync.Sandbox.RepoTest do
  use ExUnit.Case,
    async: true,
    parameterize: [
      %{ownership_model: :checkout},
      %{ownership_model: :owner}
    ]

  @moduletag :sandbox

  @todos [
    table: {
      "todos",
      [
        "id int8 not null primary key generated always as identity",
        "title text",
        "completed boolean default false"
      ]
    },
    data: {
      Support.Todo,
      ["title", "completed"],
      [["one", false], ["two", false], ["three", true]]
    }
  ]

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)

  alias Support.SandboxRepo, as: Repo
  alias Support.Todo

  import Ecto.Query, only: [from: 2]

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

  defp with_repo_data(ctx) do
    case Map.get(ctx, :data, nil) do
      {schema, columns, values} ->
        Enum.each(values, fn row_values ->
          todo =
            struct(
              schema,
              Enum.zip(columns, row_values) |> Enum.map(fn {c, v} -> {String.to_atom(c), v} end)
            )

          Repo.insert(todo)
        end)

        :ok

      nil ->
        :ok
    end
  end

  setup(ctx) do
    Ecto.Adapters.SQL.Sandbox.mode(Support.SandboxRepo, :manual)

    case ctx.ownership_model do
      :checkout ->
        :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
        Phoenix.Sync.Sandbox.start!(Repo, tags: ctx)

      :owner ->
        owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo)
        on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
        Phoenix.Sync.Sandbox.start!(Repo, owner, tags: ctx)
    end
  end

  setup [
    :with_repo_table,
    :with_repo_data
  ]

  describe "update_all" do
    @describetag @todos

    test "simple" do
      receive_sandbox_updates(fn ->
        assert {3, nil} =
                 from(t in Todo,
                   update: [
                     set: [completed: true, title: fragment("upper(?)", t.title)]
                   ]
                 )
                 |> Repo.update_all([])
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "ONE", completed: true},
                        old_value: %Support.Todo{title: "one", completed: false},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 2, title: "TWO", completed: true},
                        old_value: %Support.Todo{title: "two", completed: false},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }}

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 3, title: "THREE", completed: true},
                        old_value: %Support.Todo{title: "three"},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }}
    end

    test "with returning" do
      receive_sandbox_updates(fn ->
        assert {3, [%Todo{id: 1}, %Todo{id: 2}, %Todo{id: 3}]} =
                 from(t in Todo,
                   update: [
                     set: [completed: true, title: fragment("upper(?)", t.title)]
                   ],
                   select: [:id, :completed]
                 )
                 |> Repo.update_all([])
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "ONE", completed: true},
                        old_value: %Support.Todo{title: "one", completed: false},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 2, title: "TWO", completed: true},
                        old_value: %Support.Todo{title: "two", completed: false},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }}

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 3, title: "THREE", completed: true},
                        old_value: %Support.Todo{
                          title: "three"
                        },
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }}
    end

    test "with joins" do
      receive_sandbox_updates(fn ->
        assert {3, nil} =
                 from(t in Todo,
                   join: o in Todo,
                   on: o.id == t.id,
                   update: [
                     set: [completed: true, title: fragment("upper(?)", o.title)]
                   ]
                 )
                 |> Repo.update_all([])
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "ONE", completed: true},
                        old_value: %Support.Todo{title: "one", completed: false},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 2, title: "TWO", completed: true},
                        old_value: %Support.Todo{title: "two", completed: false},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }}

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 3, title: "THREE", completed: true},
                        old_value: %Support.Todo{
                          title: "three"
                        },
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }}
    end
  end

  describe "insert_all" do
    @describetag @todos

    test "simple" do
      receive_sandbox_updates(fn ->
        assert {2, nil} =
                 Repo.insert_all(Todo, [%{title: "more"}, %{title: "even more"}])
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 4, title: "more", completed: false},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 5, title: "even more", completed: false},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }}
    end

    test "returning" do
      receive_sandbox_updates(fn ->
        assert {2, [%Todo{title: "more"}, %Todo{title: "even more"}]} =
                 Repo.insert_all(Todo, [%{title: "more"}, %{title: "even more"}],
                   returning: [:title, :completed]
                 )
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 4, title: "more", completed: false},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 5, title: "even more", completed: false},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }}
    end
  end

  describe "delete_all" do
    @describetag @todos

    test "simple" do
      receive_sandbox_updates(fn ->
        assert {2, nil} =
                 from(t in Todo, where: t.completed == false)
                 |> Repo.delete_all()
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "one", completed: false},
                        headers: %{operation: :delete, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 2, title: "two", completed: false},
                        headers: %{operation: :delete, relation: ["public", "todos"]}
                      }}
    end

    test "returning" do
      receive_sandbox_updates(fn ->
        assert {2, [%Todo{title: "one"}, %Todo{title: "two"}]} =
                 from(t in Todo, where: t.completed == false, select: [:id, :title])
                 |> Repo.delete_all()
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "one", completed: false},
                        headers: %{operation: :delete, relation: ["public", "todos"]}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 2, title: "two", completed: false},
                        headers: %{operation: :delete, relation: ["public", "todos"]}
                      }}
    end
  end

  describe "delete" do
    @describetag @todos

    test "simple" do
      receive_sandbox_updates(fn ->
        assert {:ok, %Todo{}} = Repo.delete(%Todo{id: 1})
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "one", completed: false},
                        headers: %{operation: :delete, relation: ["public", "todos"]}
                      }},
                     1000
    end

    test "returning" do
      receive_sandbox_updates(fn ->
        assert {:ok, %Todo{id: 1}} = Repo.delete(%Todo{id: 1}, returning: [:id, :title])
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "one", completed: false},
                        headers: %{operation: :delete, relation: ["public", "todos"]}
                      }},
                     1000
    end
  end

  describe "update" do
    @describetag @todos

    test "simple" do
      receive_sandbox_updates(fn ->
        assert {:ok, %Todo{}} = Repo.update(Ecto.Changeset.change(%Todo{id: 1}, title: "changed"))
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "changed", completed: false},
                        old_value: %Support.Todo{title: "one"},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }},
                     1000
    end

    test "returning" do
      receive_sandbox_updates(fn ->
        assert {:ok, %Todo{}} =
                 Repo.update(Ecto.Changeset.change(%Todo{id: 1}, title: "changed"),
                   returning: [:id, :title]
                 )
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: 1, title: "changed", completed: false},
                        old_value: %Support.Todo{title: "one"},
                        headers: %{operation: :update, relation: ["public", "todos"]}
                      }},
                     1000
    end
  end

  describe "insert" do
    @describetag @todos

    test "simple" do
      receive_sandbox_updates(fn ->
        assert {:ok, %Todo{}} = Repo.insert(%Todo{title: "ticked", completed: true})
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: _, title: "ticked", completed: true},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000
    end

    test "returning" do
      receive_sandbox_updates(fn ->
        assert {:ok, %Todo{}} =
                 Repo.insert(%Todo{title: "ticked", completed: true},
                   returning: [:id, :completed]
                 )
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: _, title: "ticked", completed: true},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000
    end
  end

  describe "allow" do
    @describetag @todos

    test "connects any process to a stack" do
      parent = self()

      {:ok, supervisor} =
        start_supervised(
          {DynamicSupervisor, name: __MODULE__.DynamicSupervisor, strategy: :one_for_one}
        )

      receive_sandbox_updates(fn ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            supervisor,
            {Task,
             fn ->
               receive do
                 :insert ->
                   assert %Todo{} = Repo.insert!(%Todo{title: "distant"})
               end
             end}
          )

        :ok = Phoenix.Sync.Sandbox.allow(Repo, parent, pid)

        send(pid, :insert)
      end)

      # prove that the insert succeeded
      assert Repo.all(Todo) |> Enum.map(& &1.title) |> Enum.find(&(&1 == "distant"))

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: _, title: "distant"},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000
    end

    test "allows for named processes" do
      parent = self()

      {:ok, supervisor} =
        start_supervised(
          {DynamicSupervisor, name: __MODULE__.DynamicSupervisor, strategy: :one_for_one}
        )

      receive_sandbox_updates(fn ->
        task = self()

        {:ok, pid} =
          DynamicSupervisor.start_child(
            supervisor,
            {Task,
             fn ->
               Process.register(self(), :sandbox_test_process)
               send(task, :registered)

               receive do
                 :insert ->
                   assert %Todo{} = Repo.insert!(%Todo{title: "distant"})
               end
             end}
          )

        assert_receive :registered, 100

        :ok = Phoenix.Sync.Sandbox.allow(Repo, parent, :sandbox_test_process)

        send(pid, :insert)
      end)

      # prove that the insert succeeded
      assert Repo.all(Todo) |> Enum.map(& &1.title) |> Enum.find(&(&1 == "distant"))

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Support.Todo{id: _, title: "distant"},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000
    end
  end

  describe "type and embed mapping" do
    defmodule BinaryId do
      use Ecto.Schema

      @primary_key false
      schema "binary_ids" do
        field :id, :binary_id
        field :the_time, :time
        field :the_date, :date
        field :the_price, :decimal
        field :the_array, {:array, :integer}

        embeds_one :embedded, Thing do
          field :name, :string
          field :timestamp, :utc_datetime
        end

        timestamps(type: :utc_datetime)
      end
    end

    @tag table: {
           "binary_ids",
           [
             "id uuid not null",
             "embedded jsonb",
             "inserted_at timestamp with time zone",
             "updated_at timestamp with time zone",
             "the_time time",
             "the_date date",
             "the_price decimal(10, 2)",
             "the_array integer[]"
           ]
         },
         data: {
           BinaryId,
           [
             "id",
             "embedded",
             "inserted_at",
             "updated_at",
             "the_time",
             "the_date",
             "the_price",
             "the_array"
           ],
           [
             [
               "6a939b05-f467-442b-ad30-de81df681b3e",
               %{
                 __struct__: BinaryId.Thing,
                 id: "c65ae689-3cbe-4e41-8da6-7b212d26b587",
                 name: "thing 1",
                 timestamp: ~U[2025-01-01T12:25:17Z]
               },
               ~U[2025-08-12T16:34:04Z],
               ~U[2025-08-12T16:34:05Z],
               ~T[13:24:00],
               ~D[2025-01-02],
               Decimal.new("6.99"),
               [1, 2, 1]
             ],
             [
               "74828fe4-1339-420a-8c37-f474900d62d5",
               %{
                 __struct__: BinaryId.Thing,
                 id: "788f7861-0116-4da7-b218-b36e97c6d478",
                 name: "thing 2",
                 timestamp: ~U[2025-02-02T12:25:17Z]
               },
               ~U[2025-08-13T17:44:04Z],
               ~U[2025-08-13T17:44:05Z],
               ~T[13:25:01],
               ~D[2025-02-02],
               Decimal.new("9.99"),
               [2, 3, 2]
             ]
           ]
         }
    @tag encoding: true
    test "uuids" do
      parent = self()

      {:ok, client} = Phoenix.Sync.Sandbox.client()

      start_supervised!(
        {Task,
         fn ->
           for msg <- Electric.Client.stream(client, BinaryId, replica: :full),
               do: send(parent, {:change, msg})
         end}
      )

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %BinaryId{
                          id: "6a939b05-f467-442b-ad30-de81df681b3e",
                          embedded: %BinaryId.Thing{
                            id: "c65ae689-3cbe-4e41-8da6-7b212d26b587",
                            name: "thing 1",
                            timestamp: ~U[2025-01-01T12:25:17Z]
                          },
                          inserted_at: ~U[2025-08-12T16:34:04Z],
                          updated_at: ~U[2025-08-12T16:34:05Z],
                          the_time: ~T[13:24:00],
                          the_date: ~D[2025-01-02],
                          the_price: %Decimal{exp: -2, sign: 1, coef: 699},
                          the_array: [1, 2, 1]
                        },
                        headers: %{operation: :insert}
                      }},
                     1000

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %BinaryId{
                          id: "74828fe4-1339-420a-8c37-f474900d62d5",
                          embedded: %BinaryId.Thing{
                            id: "788f7861-0116-4da7-b218-b36e97c6d478",
                            name: "thing 2",
                            timestamp: ~U[2025-02-02T12:25:17Z]
                          },
                          inserted_at: ~U[2025-08-13T17:44:04Z],
                          updated_at: ~U[2025-08-13T17:44:05Z],
                          the_time: ~T[13:25:01],
                          the_date: ~D[2025-02-02],
                          the_price: %Decimal{exp: -2, sign: 1, coef: 999},
                          the_array: [2, 3, 2]
                        },
                        headers: %{operation: :insert}
                      }}

      assert_receive {:change,
                      %Electric.Client.Message.ControlMessage{
                        control: :up_to_date
                      }}

      Repo.transaction(fn ->
        Repo.insert!(%BinaryId{
          id: "778247a6-2dcb-4278-b696-8e1b974cf073",
          embedded: %BinaryId.Thing{
            id: "b266d1aa-cd5a-4bbe-8766-d7f7c041ffb3",
            name: "thing 3",
            timestamp: ~U[2025-03-03T12:25:17Z]
          },
          inserted_at: ~U[2025-08-13T18:44:04Z],
          updated_at: ~U[2025-08-13T18:44:05Z],
          the_time: ~T[14:34:00],
          the_date: ~D[2025-03-02],
          the_price: Decimal.new("16.51"),
          the_array: [1, 2, 3]
        })
      end)

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %BinaryId{
                          id: "778247a6-2dcb-4278-b696-8e1b974cf073",
                          embedded: %BinaryId.Thing{
                            id: "b266d1aa-cd5a-4bbe-8766-d7f7c041ffb3",
                            name: "thing 3",
                            timestamp: ~U[2025-03-03T12:25:17Z]
                          },
                          inserted_at: ~U[2025-08-13T18:44:04Z],
                          updated_at: ~U[2025-08-13T18:44:05Z],
                          the_time: ~T[14:34:00],
                          the_array: [1, 2, 3]
                        },
                        headers: %{operation: :insert}
                      }}
    end
  end

  defp receive_sandbox_updates(write_fun) do
    parent = self()
    ref = make_ref()

    {:ok, task_supervisor} = start_supervised(Task.Supervisor)

    task =
      Task.Supervisor.async(task_supervisor, fn ->
        receive do
          {:ready, ^ref} -> Repo.transaction(write_fun)
        end
      end)

    {:ok, client} = Phoenix.Sync.Sandbox.client()

    start_supervised!(
      {Task,
       fn ->
         for msg <- Electric.Client.stream(client, Todo, replica: :full),
             do: send(parent, {:change, msg})
       end}
    )

    assert_receive {:change,
                    %Electric.Client.Message.ChangeMessage{
                      value: %Support.Todo{id: 1, title: "one", completed: false},
                      headers: %{operation: :insert}
                    }},
                   1000

    assert_receive {:change,
                    %Electric.Client.Message.ChangeMessage{
                      value: %Support.Todo{id: 2, title: "two", completed: false},
                      headers: %{operation: :insert}
                    }}

    assert_receive {:change,
                    %Electric.Client.Message.ChangeMessage{
                      value: %Support.Todo{id: 3, title: "three", completed: true},
                      headers: %{operation: :insert}
                    }}

    assert_receive {:change,
                    %Electric.Client.Message.ControlMessage{
                      control: :up_to_date
                    }}

    send(task.pid, {:ready, ref})

    Task.await(task)
  end
end
