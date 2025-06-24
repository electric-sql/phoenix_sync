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
      get "/interruptable", TodoController, :interruptable
      get "/interruptible", TodoController, :interruptible
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

  describe "interrupt" do
    alias Phoenix.Sync.Controller
    alias Phoenix.Sync.ShapeRequestRegistry
    alias Phoenix.Sync.PredefinedShape

    @describetag interrupt: true
    @describetag long_poll_timeout: 2_000

    for path <- ~w(/todos/interruptable /todos/interruptible) do
      test "exits a long-poll request immediately #{path}", _ctx do
        resp =
          Phoenix.ConnTest.build_conn()
          |> Phoenix.ConnTest.get(unquote(path), %{offset: "-1"})

        assert resp.status == 200
        assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

        assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

        resp =
          Phoenix.ConnTest.build_conn()
          |> Phoenix.ConnTest.get(unquote(path), %{offset: "0_0", handle: handle})

        assert resp.status == 200
        assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_inf"]

        assert [^handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

        task =
          Task.async(fn ->
            Phoenix.ConnTest.build_conn()
            |> Phoenix.ConnTest.get(unquote(path), %{
              offset: "0_inf",
              handle: handle,
              live: "true"
            })
          end)

        # let the request start and register itself
        Process.sleep(100)

        assert {:ok, 1} = Controller.interrupt_matching(table: "todos")

        # in http mode this test only terminates when the client's request to the
        # backend completes even though we've terminated the calling process
        # which is why the long_poll_timeout is set to 2 seconds and not higher
        # and this await timeout is so low
        response = Task.await(task, 100)

        assert 200 == response.status
        assert ["0_inf"] = Plug.Conn.get_resp_header(response, "electric-offset")
        assert [^handle] = Plug.Conn.get_resp_header(response, "electric-handle")
        assert ["application/json" <> _] = Plug.Conn.get_resp_header(response, "content-type")
        assert [cache_control] = Plug.Conn.get_resp_header(response, "cache-control")
        assert cache_control =~ "no-cache"
        assert cache_control =~ "no-store"

        assert [] = ShapeRequestRegistry.registered_requests()
      end
    end

    test "shape matching w/params" do
      defns = [
        [[table: "todos"]],
        [[table: "todos"]],
        [[table: "todos", where: "completed = $1"]],
        [[table: "todos", where: "completed = $1", params: [true]]],
        [[table: "todos", where: "completed = $1", params: %{1 => true}]]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          where: "completed = $1",
          params: [true]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} == apply(Controller, :interrupt_matching, args),
               "failed to match shape spec #{inspect(args)}"

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching w/namespace" do
      import Ecto.Query, only: [from: 2]

      defns = [
        [[table: "todos"]],
        [[namespace: "public", table: "todos"]],
        [[table: "todos", where: "completed = true"]],
        [Ecto.Query.put_query_prefix(Support.Todo, "public"), [where: "completed = true"]],
        [
          fn %{shape_config: config} ->
            config[:table] == "todos"
          end,
          []
        ]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          namespace: "public",
          where: "completed = true",
          columns: ["id", "title", "completed"]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Controller, :interrupt_matching, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching against Ecto.Query" do
      import Ecto.Query, only: [from: 2]

      defns = [
        [[table: "todos"]],
        [[namespace: "public", table: "todos"]],
        [from(t in Support.Todo, prefix: "public", where: t.completed == true), []],
        [
          fn %PredefinedShape{} = shape ->
            params = PredefinedShape.to_shape_params(shape)
            params[:table] == "todos"
          end,
          []
        ]
      ]

      shape =
        PredefinedShape.new!(
          from(t in Support.Todo, prefix: "public", where: t.completed == true)
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Controller, :interrupt_matching, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching w/o namespace" do
      defns = [
        ["todos"],
        [[table: "todos"]],
        [[table: "todos", where: "completed = true"]],
        [Support.Todo, [where: "completed = true"]]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          where: "completed = true",
          columns: ["id", "title", "completed"]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Controller, :interrupt_matching, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end
  end
end
