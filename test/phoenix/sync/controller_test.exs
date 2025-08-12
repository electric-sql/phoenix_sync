defmodule Phoenix.Sync.ControllerTest do
  use ExUnit.Case,
    async: false,
    parameterize: [
      %{
        sync_config: [
          env: :test,
          mode: :embedded,
          pool_opts: [backoff_type: :stop, max_restarts: 0, pool_size: 2]
        ]
      },
      %{
        sync_config: [
          mode: :http,
          env: :test,
          url: "http://localhost:3000",
          pool_opts: [backoff_type: :stop, max_restarts: 0, pool_size: 2]
        ]
      }
    ]

  use Support.ElectricHelpers, endpoint: __MODULE__.Endpoint

  import Plug.Test

  require Phoenix.ConnTest

  defmodule Router do
    use Phoenix.Router

    pipeline :browser do
      plug :accepts, ["html"]
    end

    scope "/todos", Phoenix.Sync.LiveViewTest do
      pipe_through [:browser]

      get "/all", TodoController, :all
      get "/complete", TodoController, :complete
      get "/flexible", TodoController, :flexible
      get "/module", TodoController, :module
      get "/changeset", TodoController, :changeset
      get "/complex", TodoController, :complex
      get "/interruptible", TodoController, :interruptible
      get "/interruptible_dynamic", TodoController, :interruptible_dynamic
    end
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :phoenix_sync

    plug Router
  end

  Code.ensure_loaded!(Support.Todo)
  Code.ensure_loaded!(Support.Repo)

  @moduletag table: {
               "todos",
               [
                 "id int8 not null primary key generated always as identity",
                 "title text",
                 "completed boolean default false"
               ]
             }
  @moduletag data:
               {"todos", ["title", "completed"],
                [["one", false], ["two", false], ["three", true]]}

  setup [
    :define_endpoint,
    :with_stack_id_from_test,
    :with_unique_db,
    :with_stack_config,
    :with_table,
    :with_data,
    :start_embedded,
    :configure_endpoint
  ]

  describe "phoenix: sync_render/3" do
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

    test "includes CORS headers", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/all", %{offset: "-1"})

      assert resp.status == 200
      assert [expose] = Plug.Conn.get_resp_header(resp, "access-control-expose-headers")
      assert String.contains?(expose, "electric-offset")
    end

    test "supports where clauses", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/complete", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "allows for ecto queries", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/flexible", %{offset: "-1", completed: true})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/flexible", %{offset: "-1", completed: false})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "allows for ecto schema module", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/module", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "allows for changeset function", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/changeset", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "allows for complex shapes", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/complex", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}}
             ] = Jason.decode!(resp.resp_body)
    end
  end

  defmodule PlugRouter do
    use Plug.Router, copy_opts_to_assign: :options
    use Phoenix.Sync.Controller

    plug :match
    plug :dispatch

    get "/shape/todos" do
      sync_render(conn, table: "todos")
    end

    get "/shape/interruptible-todos" do
      sync_render(conn, fn ->
        shape_params = Agent.get(:interruptible_dynamic_agent, & &1)

        Phoenix.Sync.shape!(
          table: "todos",
          where: "completed = $1",
          params: shape_params
        )
      end)
    end
  end

  describe "plug: sync_render/3" do
    setup(ctx) do
      [plug_opts: [phoenix_sync: Phoenix.Sync.plug_opts(ctx.electric_opts)]]
    end

    test "returns the sync events", ctx do
      conn = conn(:get, "/shape/todos", %{"offset" => "-1"})

      resp = PlugRouter.call(conn, PlugRouter.init(ctx.plug_opts))

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "includes content-type header", ctx do
      conn = conn(:get, "/shape/todos", %{"offset" => "-1"})

      resp = PlugRouter.call(conn, PlugRouter.init(ctx.plug_opts))

      assert Plug.Conn.get_resp_header(resp, "content-type") == [
               "application/json; charset=utf-8"
             ]
    end

    test "includes CORS headers", ctx do
      conn = conn(:get, "/shape/todos", %{"offset" => "-1"})

      resp = PlugRouter.call(conn, PlugRouter.init(ctx.plug_opts))

      assert [expose] = Plug.Conn.get_resp_header(resp, "access-control-expose-headers")
      assert String.contains?(expose, "electric-offset")
    end
  end

  describe "plug: interruptible sync_render/3" do
    alias Phoenix.Sync.ShapeRequestRegistry

    @describetag interrupt: true
    @describetag long_poll_timeout: 5_000

    setup(ctx) do
      [plug_opts: [phoenix_sync: Phoenix.Sync.plug_opts(ctx.electric_opts)]]
    end

    test "re-tries the request after an interrupt", ctx do
      agent = start_supervised!({Agent, fn -> [false] end})

      Process.register(agent, :interruptible_dynamic_agent)

      conn = conn(:get, "/shape/interruptible-todos", %{"offset" => "-1"})

      resp = PlugRouter.call(conn, PlugRouter.init(ctx.plug_opts))

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]
      assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      conn = conn(:get, "/shape/interruptible-todos", %{"offset" => "0_0", "handle" => handle})
      resp = PlugRouter.call(conn, PlugRouter.init(ctx.plug_opts))

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_inf"]

      task =
        Task.async(fn ->
          conn =
            conn(:get, "/shape/interruptible-todos", %{
              "offset" => "0_inf",
              "handle" => handle,
              "live" => "true"
            })

          PlugRouter.call(conn, PlugRouter.init(ctx.plug_opts))
        end)

      # let the request start and register itself
      Process.sleep(100)

      # change the shape definition while the request is running
      Agent.update(:interruptible_dynamic_agent, fn _ -> [true] end)

      # interrupt forcing a re-request which will pick up the changed shape definition
      assert {:ok, 1} = Phoenix.Sync.interrupt(table: "todos")

      response = Task.await(task, 1000)

      assert 409 == response.status

      assert [%{"headers" => %{"control" => "must-refetch"}}] = Jason.decode!(response.resp_body)

      assert [] = ShapeRequestRegistry.registered_requests()
    end
  end

  describe "interruptible" do
    alias Phoenix.Sync.ShapeRequestRegistry

    @describetag interrupt: true
    @describetag long_poll_timeout: 5_000

    test "retries an interrupted long-poll", ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/interruptible", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/interruptible", %{offset: "0_0", handle: handle})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_inf"]

      assert [^handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      task =
        Task.async(fn ->
          Phoenix.ConnTest.build_conn()
          |> Phoenix.ConnTest.get("/todos/interruptible", %{
            offset: "0_inf",
            handle: handle,
            live: "true"
          })
        end)

      # let the request start and register itself
      Process.sleep(100)

      assert {:ok, 1} = Phoenix.Sync.interrupt(table: "todos")

      Process.sleep(100)

      Postgrex.query!(
        ctx.db_conn,
        "INSERT INTO todos (title, completed) VALUES ($1, $2)",
        ["new todo", false]
      )

      # in http mode this test only terminates when the client's request to the
      # backend completes even though we've terminated the calling process
      # which is why the long_poll_timeout is set to 2 seconds and not higher
      # and this await timeout is so low
      response = Task.await(task, 500)

      assert 200 == response.status

      assert [%{"value" => %{"title" => "new todo"}}, %{"headers" => %{}}] =
               Jason.decode!(response.resp_body)

      assert [_offset] = Plug.Conn.get_resp_header(response, "electric-offset")
      assert [cache_control] = Plug.Conn.get_resp_header(response, "cache-control")
      assert cache_control =~ "public"
      assert cache_control =~ "max-age=5"

      assert [] = ShapeRequestRegistry.registered_requests()
    end

    test "include cors headers" do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/interruptible", %{offset: "-1"})

      assert resp.status == 200
      assert [expose] = Plug.Conn.get_resp_header(resp, "access-control-expose-headers")
      assert String.contains?(expose, "electric-offset")
    end

    test "returns must-refetch for invalidated shape", _ctx do
      agent = start_supervised!({Agent, fn -> [false] end})

      Process.register(agent, :interruptible_dynamic_agent)

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/interruptible_dynamic", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/todos/interruptible_dynamic", %{offset: "0_0", handle: handle})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_inf"]

      assert [^handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      task =
        Task.async(fn ->
          Phoenix.ConnTest.build_conn()
          |> Phoenix.ConnTest.get("/todos/interruptible_dynamic", %{
            offset: "0_inf",
            handle: handle,
            live: "true"
          })
        end)

      # let the request start and register itself
      Process.sleep(100)

      # change the shape definition while the request is running
      Agent.update(:interruptible_dynamic_agent, fn _ -> [true] end)

      # interrupt forcing a re-request which will pick up the changed shape definition
      assert {:ok, 1} = Phoenix.Sync.interrupt(table: "todos")

      response = Task.await(task, 1000)

      assert 409 == response.status

      assert [%{"headers" => %{"control" => "must-refetch"}}] = Jason.decode!(response.resp_body)

      assert [] = ShapeRequestRegistry.registered_requests()
    end
  end
end
