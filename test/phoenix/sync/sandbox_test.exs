defmodule Phoenix.Sync.SandboxTest do
  use ExUnit.Case, async: true
  use Support.ElectricHelpers, endpoint: __MODULE__.Endpoint

  @moduletag :sandbox

  Code.ensure_loaded!(Support.SandboxRepo)
  Code.ensure_loaded!(Support.Todo)
  Code.ensure_loaded!(Support.User)

  defmodule Controller do
    use Phoenix.Controller, formats: [:html, :json]

    import Plug.Conn
    import Phoenix.Sync.Controller

    def all(conn, params) do
      sync_render(conn, params, table: "todos")
    end
  end

  defmodule Router do
    use Phoenix.Router

    import Phoenix.Sync.Router
    require Phoenix.LiveView.Router

    scope "/sync" do
      # by default we take the table name from the path
      # note that this does not handle weird table names that need quoting
      # or namespaces
      sync "/todos", Support.Todo
    end

    scope "/stream" do
      Phoenix.LiveView.Router.live("/sandbox", Phoenix.Sync.LiveViewTest.StreamSandbox)
    end

    scope "/todos" do
      get "/all", Controller, :all
    end
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :phoenix_sync

    plug Router
  end

  alias Support.SandboxRepo, as: Repo
  alias Support.Todo

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest
  import Plug.Conn

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
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)

    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
    Phoenix.Sync.Sandbox.start!(Repo, owner, tags: ctx)
  end

  setup [
    :define_endpoint,
    :with_repo_table,
    :with_repo_data
  ]

  setup(ctx) do
    endpoint = Map.get(ctx, :endpoint, @endpoint)

    Phoenix.Config.put(endpoint, :phoenix_sync, Phoenix.Sync.plug_opts())

    [endpoint: endpoint]
  end

  @moduletag table: {
               "todos",
               [
                 "id int8 not null primary key generated always as identity",
                 "title text",
                 "completed boolean default false"
               ]
             }
  @moduletag data: {
               Support.Todo,
               ["title", "completed"],
               [["one", false], ["two", false], ["three", true]]
             }

  test "live view", _ctx do
    {:ok, lv, html} =
      build_conn()
      |> put_private(:test_pid, self())
      |> live("/stream/sandbox")

    assert_receive {:sync, {:todos, :loaded}}
    assert_receive {:sync, {:todos, :live}}

    for todo <- Repo.all(Todo) do
      assert html =~ todo.title
    end

    Repo.insert!(%Todo{title: "fourth", completed: false})

    assert_receive {:sync, _event}

    assert render(lv) =~ "fourth"
  end

  test "Client.stream" do
    parent = self()
    ref = make_ref()

    task =
      Task.async(fn ->
        receive do
          {:ready, ^ref} ->
            Repo.transaction(fn ->
              Repo.insert!(%Todo{title: "super"})
            end)
        end
      end)

    start_supervised!(
      {Task,
       fn ->
         for msg <- Phoenix.Sync.Client.stream(Todo, replica: :full),
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

    assert_receive {:change,
                    %Electric.Client.Message.ChangeMessage{
                      value: %Support.Todo{id: _, title: "super"},
                      headers: %{operation: :insert}
                    }}
  end

  describe "Phoenix.Sync.Router sandbox integration" do
    @describetag :router

    test "basic sync" do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/todos", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "live changes" do
      parent = self()

      task1 =
        Task.async(fn ->
          resp =
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get("/sync/todos", %{offset: "-1"})

          assert resp.status == 200
          assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]
          [offset] = Plug.Conn.get_resp_header(resp, "electric-offset")
          [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

          snapshot = Jason.decode!(resp.resp_body)

          resp =
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get("/sync/todos", %{offset: offset, handle: handle})

          assert [%{"headers" => %{"control" => "up-to-date", "global_last_seen_lsn" => "0"}}] =
                   Jason.decode!(resp.resp_body)

          [offset] = Plug.Conn.get_resp_header(resp, "electric-offset")
          [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

          send(parent, {:snapshot, snapshot})
          receive(do: (:request -> :ok))

          resp =
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get("/sync/todos?live=true", %{offset: offset, handle: handle})

          assert resp.status == 200

          send(parent, {:live, Jason.decode!(resp.resp_body)})
        end)

      task2 =
        Task.async(fn ->
          receive do
            :insert ->
              Repo.transaction(fn ->
                Repo.insert!(%Todo{title: "wild"})
              end)
          end
        end)

      snapshot = receive(do: ({:snapshot, response} -> response))

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = snapshot

      send(task1.pid, :request)
      send(task2.pid, :insert)
      live = receive(do: ({:live, response} -> response))

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "wild"}},
               %{"headers" => %{"control" => "up-to-date"}}
             ] = live

      Task.await_many([task1, task2])
    end
  end

  describe "Phoenix.Sync.Controller sandbox integration" do
    @describetag :controller

    test "returns the shape data", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/all", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "live changes" do
      parent = self()
      path = "/todos/all"

      task1 =
        Task.async(fn ->
          resp =
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get(path, %{offset: "-1"})

          assert resp.status == 200
          assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]
          [offset] = Plug.Conn.get_resp_header(resp, "electric-offset")
          [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

          snapshot = Jason.decode!(resp.resp_body)

          resp =
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get(path, %{offset: offset, handle: handle})

          assert [%{"headers" => %{"control" => "up-to-date", "global_last_seen_lsn" => "0"}}] =
                   Jason.decode!(resp.resp_body)

          [offset] = Plug.Conn.get_resp_header(resp, "electric-offset")
          [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

          send(parent, {:snapshot, snapshot})
          receive(do: (:request -> :ok))

          resp =
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get(path, %{live: true, offset: offset, handle: handle})

          assert resp.status == 200

          send(parent, {:live, Jason.decode!(resp.resp_body)})
        end)

      task2 =
        Task.async(fn ->
          receive do
            :insert ->
              Repo.transaction(fn ->
                Repo.insert!(%Todo{title: "wild"})
              end)
          end
        end)

      snapshot = receive(do: ({:snapshot, response} -> response))

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = snapshot

      send(task1.pid, :request)
      send(task2.pid, :insert)
      live = receive(do: ({:live, response} -> response))

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "wild"}},
               %{"headers" => %{"control" => "up-to-date"}}
             ] = live

      Task.await_many([task1, task2])
    end
  end
end
