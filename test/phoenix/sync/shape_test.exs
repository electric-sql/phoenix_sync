defmodule Support.ShapeTest do
  use ExUnit.Case, async: true
  use Support.RepoSetup, repo: Support.SandboxRepo

  alias Phoenix.Sync.Shape

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)

  alias Support.SandboxRepo, as: Repo
  alias Support.Todo

  setup(ctx) do
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)
    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)

    if Map.get(ctx, :sandbox, true), do: Phoenix.Sync.Sandbox.start!(Repo, owner, tags: ctx)

    :ok
  end

  @todos table: {
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

  setup [
    :with_repo_table,
    :with_repo_data
  ]

  defp start_shape(module, opts) when is_list(opts) do
    start_shape([module | opts])
  end

  defp start_shape(opts) do
    {:ok, pid} = start_supervised({Shape, opts})

    if Phoenix.Sync.Sandbox.stack_id() do
      Phoenix.Sync.Sandbox.allow(Repo, self(), pid)
    end

    {:ok, pid}
  end

  @moduletag @todos

  setup(_ctx) do
    if Phoenix.Sync.Sandbox.stack_id() do
      {:ok, client} = Phoenix.Sync.Sandbox.client()
      [client: client]
    else
      :ok
    end
  end

  describe "start_link/2" do
    test "starts a shape process with the given queryable", ctx do
      {:ok, pid} = Shape.start_link(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000
    end
  end

  describe "sync updates" do
    @describetag streaming: true

    test "supports an Ecto query stream", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert [
               {"\"public\".\"todos\"/\"1\"",
                %Support.Todo{
                  id: 1,
                  title: "one",
                  completed: false
                }},
               {"\"public\".\"todos\"/\"2\"",
                %Support.Todo{
                  id: 2,
                  title: "two",
                  completed: false
                }},
               {"\"public\".\"todos\"/\"3\"",
                %Support.Todo{
                  id: 3,
                  title: "three",
                  completed: true
                }}
             ] = Shape.to_list(pid)
    end

    test "updates are applied", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      %Todo{} = todo = Repo.get(Todo, 1)

      Repo.transaction(fn ->
        Repo.update!(Ecto.Changeset.change(todo, title: "updated title", completed: true))
      end)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert [
               {"\"public\".\"todos\"/\"1\"",
                %Support.Todo{
                  id: 1,
                  title: "updated title",
                  completed: true
                }},
               {"\"public\".\"todos\"/\"2\"",
                %Support.Todo{
                  id: 2,
                  title: "two",
                  completed: false
                }},
               {"\"public\".\"todos\"/\"3\"",
                %Support.Todo{
                  id: 3,
                  title: "three",
                  completed: true
                }}
             ] = Shape.to_list(pid)
    end

    test "deletes are applied", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      %Todo{} = todo = Repo.get(Todo, 1)

      Repo.transaction(fn ->
        Repo.delete!(todo)
      end)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert [
               {"\"public\".\"todos\"/\"2\"",
                %Support.Todo{
                  id: 2,
                  title: "two",
                  completed: false
                }},
               {"\"public\".\"todos\"/\"3\"",
                %Support.Todo{
                  id: 3,
                  title: "three",
                  completed: true
                }}
             ] = Shape.to_list(pid)
    end

    test "must refetch messages clear the state", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(pid, only: [:up_to_date, :must_refetch], tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      Phoenix.Sync.Sandbox.must_refetch!(Support.Todo)

      assert_receive {:my_sync, ^ref, :must_refetch}, 2000

      assert [] = Shape.to_list(pid)
    end

    test "supports keyword-based shapes", ctx do
      {:ok, pid} = start_shape(table: "todos", client: ctx.client)

      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert [
               {"\"public\".\"todos\"/\"1\"",
                %{"completed" => "false", "id" => 1, "title" => "one"}},
               {"\"public\".\"todos\"/\"2\"",
                %{"completed" => "false", "id" => 2, "title" => "two"}},
               {"\"public\".\"todos\"/\"3\"",
                %{"completed" => "true", "id" => 3, "title" => "three"}}
             ] = Shape.to_list(pid)
    end

    test "allows for naming the shape process", ctx do
      {:ok, pid} = start_shape([Support.Todo, name: __MODULE__.Shape45, client: ctx.client])

      Phoenix.Sync.Sandbox.allow(Repo, self(), __MODULE__.Shape45)

      assert ^pid = GenServer.whereis(__MODULE__.Shape45)
    end

    test "raises if an invalid queryable is provided", ctx do
      assert_raise MatchError, fn ->
        start_shape(:invalid_queryable, client: ctx.client)
      end
    end
  end

  describe "outside of a running sandbox" do
    @describetag sandbox: false

    test "emits a single up_to_date message simulating an empty shape", _ctx do
      {:ok, pid} = start_shape(Support.Todo)

      ref = Shape.subscribe(pid, only: [:up_to_date, :must_refetch], tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert [] = Shape.to_list(pid)
    end
  end

  describe "all/2" do
    @describetag all: true

    test "keys: false returns only the values", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert [
               %Support.Todo{id: 1, title: "one", completed: false},
               %Support.Todo{id: 2, title: "two", completed: false},
               %Support.Todo{id: 3, title: "three", completed: true}
             ] = Shape.to_list(pid, keys: false)
    end
  end

  describe "stream/2" do
    @describetag stream: true

    test "returns a lazily evaluated stream of {key, value} pairs", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      stream = Shape.stream(pid)

      assert Enum.into(stream, []) == Shape.to_list(pid)
    end

    test "removes keys if keys: false", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      stream = Shape.stream(pid, keys: false)

      assert Enum.into(stream, []) == Shape.to_list(pid, keys: false)
    end

    test "returns an empty stream if the shape is empty", ctx do
      import Ecto.Query, only: [from: 2]

      {:ok, pid} =
        start_shape(from(t in Support.Todo, where: t.title == "missing"), client: ctx.client)

      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      stream = Shape.stream(pid)

      assert Enum.into(stream, []) == []
    end
  end

  describe "to_map" do
    @describetag to_map: true

    test "returns a map of key -> value", ctx do
      {:ok, pid} = start_shape(table: "todos", client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert %{
               "\"public\".\"todos\"/\"1\"" => %{
                 "completed" => "false",
                 "id" => 1,
                 "title" => "one"
               },
               "\"public\".\"todos\"/\"2\"" => %{
                 "completed" => "false",
                 "id" => 2,
                 "title" => "two"
               },
               "\"public\".\"todos\"/\"3\"" => %{
                 "completed" => "true",
                 "id" => 3,
                 "title" => "three"
               }
             } = Shape.to_map(pid)
    end

    test "allows for specifying the map transform", ctx do
      {:ok, pid} = start_shape(table: "todos", client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert %{1 => "one", 2 => "two", 3 => "three"} =
               Shape.to_map(pid, fn {"\"public\".\"todos\"/" <> _,
                                     %{"id" => id, "title" => title}} ->
                 {id, title}
               end)
    end
  end

  describe "find/2" do
    @describetag find: true

    test "returns the first item that matches the test", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      assert %Support.Todo{id: 1, title: "one", completed: false} =
               Shape.find(pid, fn %{id: id} -> id == 1 end)
    end

    test "returns default if none match", ctx do
      {:ok, pid} = start_shape(Support.Todo, client: ctx.client)
      ref = Shape.subscribe(pid, only: :up_to_date, tag: :my_sync)
      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      refute Shape.find(pid, fn %{id: id} -> id == -1 end)
      assert :error == Shape.find(pid, :error, fn %{id: id} -> id == -1 end)
    end
  end

  describe "subscription" do
    test "allows for subscribing to a stream and receiving events", ctx do
      {:ok, shape} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(shape)

      assert_receive {:sync, ^ref, {:insert, {"\"public\".\"todos\"/\"1\"", %Todo{id: 1}}}}, 500
      assert_receive {:sync, ^ref, {:insert, {"\"public\".\"todos\"/\"2\"", %Todo{id: 2}}}}
      assert_receive {:sync, ^ref, {:insert, {"\"public\".\"todos\"/\"3\"", %Todo{id: 3}}}}
      assert_receive {:sync, ^ref, :up_to_date}

      %Todo{} = todo = Repo.get(Todo, 1)

      Repo.transaction(fn ->
        new_todo = Repo.insert!(%Todo{title: "new todo"})
        Repo.update!(Ecto.Changeset.change(todo, title: "updated title", completed: true))
        Repo.delete!(new_todo)
      end)

      assert_receive {:sync, ^ref, {:insert, {new_key, %Todo{id: new_id, title: "new todo"}}}},
                     500

      assert_receive {:sync, ^ref,
                      {:update,
                       {"\"public\".\"todos\"/\"1\"", %Todo{id: 1, title: "updated title"}}}},
                     500

      assert_receive {:sync, ^ref, {:delete, {^new_key, %Todo{id: ^new_id}}}}, 500
      assert_receive {:sync, ^ref, :up_to_date}
    end

    test "allows for subscribing to a subset of events", ctx do
      {:ok, shape} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(shape, only: [:update, :up_to_date], tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      %Todo{} = todo = Repo.get(Todo, 1)

      Repo.transaction(fn ->
        Repo.insert!(%Todo{title: "new todo"})
        Repo.update!(Ecto.Changeset.change(todo, title: "updated title", completed: true))
      end)

      assert_receive {:my_sync, ^ref, {:update, _}}, 500
      assert_receive {:my_sync, ^ref, :up_to_date}
      refute_receive {:my_sync, ^ref, {:insert, _}}
    end

    test "updates are mapped to move-ins and -outs", ctx do
      import Ecto.Query, only: [from: 2]

      {:ok, shape} =
        start_shape(from(t in Support.Todo, where: t.completed == true), client: ctx.client)

      ref = Shape.subscribe(shape)

      assert_receive {:sync, ^ref, {:insert, {_, _}}}, 1000
      assert_receive {:sync, ^ref, :up_to_date}, 500

      assert [%Todo{id: 3}] = Shape.to_list(shape, keys: false)

      %Todo{} = todo1 = Repo.get(Todo, 1)
      %Todo{} = todo3 = Repo.get(Todo, 3)

      Repo.transaction(fn ->
        Repo.update!(Ecto.Changeset.change(todo1, completed: true))
        Repo.update!(Ecto.Changeset.change(todo3, completed: false))
      end)

      assert_receive {:sync, ^ref, {:insert, {_, %Todo{id: 1}}}}, 500
      assert_receive {:sync, ^ref, {:delete, {_, %Todo{id: 3}}}}, 500

      assert [%Todo{id: 1}] = Shape.to_list(shape, keys: false)
    end

    test "allows for unsubscribing from a stream", ctx do
      {:ok, shape} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(shape)

      assert_receive {:sync, ^ref, :up_to_date}, 500

      Shape.unsubscribe(shape)

      Repo.transaction(fn ->
        Repo.insert!(%Todo{title: "new todo"})
      end)

      refute_receive {:my_sync, ^ref, _}, 500
    end

    test "subscribers are removed after exit", ctx do
      parent = self()
      {:ok, shape} = start_shape(Support.Todo, client: ctx.client)

      ref = Shape.subscribe(shape, only: :up_to_date, tag: :my_sync)

      assert_receive {:my_sync, ^ref, :up_to_date}, 1000

      subscriber =
        spawn(fn ->
          Shape.subscribe(shape)
          send(parent, :ready)
          receive(do: (:quit -> :ok))
        end)

      Process.monitor(subscriber)

      assert_receive :ready, 500

      assert Shape.subscribers(shape) == [self(), subscriber]

      send(subscriber, :quit)

      assert_receive {:DOWN, _, :process, ^subscriber, _}, 500

      assert Shape.subscribers(shape) == [self()]
    end
  end

  describe "resumable streams" do
    @describetag skip: true

    test "writes progress to an ecto repo"
    test "debounces events"

    # so even if we're not persisting the data, we can persist a position
    # in the stream (assuming the handle hasn't changed)
    test "resumes stream from the last position"
    test "discards resume information if handle has changed"

    test "can resume from a given point"
    test "can resume from a given point with a custom function"
  end

  describe "Persistentence.ETS" do
    # TODO
  end

  describe "Persistentence.Ecto" do
    # TODO
  end
end
