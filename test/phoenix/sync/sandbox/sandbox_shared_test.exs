defmodule Phoenix.Sync.Sandbox.SandboxSharedTest do
  use ExUnit.Case, async: false

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)
  Code.ensure_loaded!(Support.User)

  alias Support.SandboxRepo, as: Repo
  alias Support.Todo

  @moduletag :sandbox
  @moduletag table: {
               "todos",
               [
                 "id int8 not null primary key generated always as identity",
                 "title text",
                 "completed boolean default false"
               ]
             }

  setup(ctx) do
    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
    Phoenix.Sync.Sandbox.start!(Repo, owner, shared: true, tags: ctx)
  end

  setup [
    :with_repo_table
  ]

  describe "shared sandbox" do
    test "allows any process to access the test stack" do
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
                   Repo.transaction(fn ->
                     assert {:ok, %Todo{}} = Repo.insert(%Todo{title: "fragrant"})
                     send(parent, :inserted)
                   end)
               end
             end}
          )

        send(pid, :insert)
      end)

      assert_receive :inserted, 500

      # prove that the insert succeeded
      assert Repo.all(Todo) |> Enum.map(& &1.title) |> Enum.find(&(&1 == "fragrant"))

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Todo{id: _, title: "fragrant"},
                        headers: %{operation: :insert, relation: ["public", "todos"]}
                      }},
                     1000
    end
  end

  defp receive_sandbox_updates(write_fun) do
    parent = self()
    ref = make_ref()

    task =
      Task.async(fn ->
        receive do
          {:ready, ^ref} -> write_fun.()
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

    send(task.pid, {:ready, ref})

    Task.await(task)
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
