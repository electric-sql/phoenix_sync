defmodule Phoenix.Sync.RouterTest do
  @pool_opts [backoff_type: :stop, max_restarts: 0, pool_size: 2]

  use ExUnit.Case,
    async: false,
    parameterize: [
      %{
        sync_config: [
          env: :test,
          mode: :embedded,
          pool_opts: @pool_opts
        ]
      },
      %{
        sync_config: [
          env: :test,
          mode: :http,
          url: "http://localhost:3000",
          pool_opts: @pool_opts
        ]
      }
    ]

  use Support.ElectricHelpers, endpoint: __MODULE__.Endpoint

  import Plug.Test

  require Phoenix.ConnTest

  defmodule Router do
    use Phoenix.Router

    import Phoenix.Sync.Router

    scope "/sync" do
      # by default we take the table name from the path
      # note that this does not handle weird table names that need quoting
      # or namespaces
      sync "/todos", Support.Todo

      # or we can expliclty specify the table
      sync "/things-to-do", table: "todos"

      # to use a non-standard namespace, we include it
      # so in this case the table is "food"."toeats"
      sync "/toeats", table: "toeats", namespace: "food"

      # or we can expliclty specify the table
      sync "/ideas",
        table: "ideas",
        where: "plausible = true",
        columns: ["id", "title"],
        replica: :full,
        storage: %{compaction: :disabled}

      # support shapes from a query, passed as the 2nd arg
      # #sdf
      sync "/query-where", Support.Todo, where: "completed = false"

      # or as query: ...
      sync "/query-bare", Support.Todo

      # query version also accepts shape config
      sync "/query-config", Support.Todo, replica: :full
      sync "/query-config2", Support.Todo, replica: :full, storage: %{compaction: :disabled}
    end

    scope "/namespaced-sync", WebNamespace do
      sync "/todos", Support.Todo
    end
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :phoenix_sync

    plug Router
  end

  Code.ensure_loaded!(Support.Todo)
  Code.ensure_loaded!(Support.Repo)

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

  describe "Phoenix.Router - shape/2" do
    @tag table: {
           "todos",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "completed boolean default false"
           ]
         }
    @tag data: {"todos", ["title"], [["one"], ["two"], ["three"]]}

    test "supports schema modules", _ctx do
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

    @tag table: {
           "todos",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "completed boolean default false"
           ]
         }
    @tag data: {"todos", ["title"], [["one"], ["two"], ["three"]]}
    test "supports schema modules within aliased scope", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/namespaced-sync/todos", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    @tag table: {
           "todos",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "completed boolean default false"
           ]
         }
    @tag data: {"todos", ["title"], [["one"], ["two"], ["three"]]}

    test "allows for specifying the table explicitly", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/things-to-do", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    @tag table: {
           "todos",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "completed boolean default false"
           ]
         }
    @tag data: {"todos", ["title"], [["one"], ["two"], ["three"]]}

    test "returns a correct content-type header", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/things-to-do", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert Plug.Conn.get_resp_header(resp, "content-type") == [
               "application/json; charset=utf-8"
             ]
    end

    @tag table: {
           "ideas",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "plausible boolean default false",
             "completed boolean default false"
           ]
         }
    @tag data: {
           "ideas",
           ["title", "plausible"],
           [["world peace", false], ["world war", true], ["make tea", true]]
         }

    test "allows for mixed definition using path and [where, column] modifiers", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/ideas", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "world war"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "make tea"}}
             ] = Jason.decode!(resp.resp_body)
    end

    @tag table: {
           {"food", "toeats"},
           [
             "id int8 not null primary key generated always as identity",
             "food text"
           ]
         }
    @tag data: {
           {"food", "toeats"},
           ["food"],
           [["peas"], ["beans"], ["sweetcorn"]]
         }

    test "can provide a custom namespace", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/toeats", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"food" => "peas"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"food" => "beans"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"food" => "sweetcorn"}}
             ] = Jason.decode!(resp.resp_body)
    end

    @tag table: {
           "todos",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "completed boolean default false"
           ]
         }
    @tag data: {
           "todos",
           ["title", "completed"],
           [["one", false], ["two", false], ["three", true]]
         }

    test "accepts Ecto queries as the shape definition", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-where", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}}
             ] = Jason.decode!(resp.resp_body)

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-bare", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-config", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-config2", %{offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    @tag table: {
           "todos",
           [
             "id int8 not null primary key generated always as identity",
             "title text",
             "completed boolean default false"
           ]
         }
    @tag data: {
           "todos",
           ["title", "completed"],
           [["one", false], ["two", false], ["three", true]]
         }
    test "cursor header is correctly returned", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-where", %{offset: "-1"})

      assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")
      assert [offset] = Plug.Conn.get_resp_header(resp, "electric-offset")

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-where", %{
          offset: offset,
          handle: handle,
          live: false
        })

      assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")
      assert [offset] = Plug.Conn.get_resp_header(resp, "electric-offset")
      assert [_] = Plug.Conn.get_resp_header(resp, "electric-up-to-date")

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/sync/query-where", %{
          offset: offset,
          handle: handle,
          live: true,
          cursor: ""
        })

      assert ["0"] = Plug.Conn.get_resp_header(resp, "electric-cursor")
    end
  end

  describe "Plug.Router - shape/2" do
    @describetag table: {
                   "todos",
                   [
                     "id int8 not null primary key generated always as identity",
                     "title text",
                     "completed boolean default false",
                     "plausible boolean default false"
                   ]
                 }
    @describetag data: {
                   "todos",
                   ["title", "plausible"],
                   [["one", true], ["two", true], ["three", true]]
                 }

    defmodule MyScope do
      use Plug.Router
      use Phoenix.Sync.Router, opts_in_assign: :options

      plug :match
      plug :dispatch

      sync "/todos", Support.Todo
    end

    defmodule MyRouter do
      use Plug.Router, copy_opts_to_assign: :options
      use Phoenix.Sync.Router

      plug :match
      plug :dispatch

      get "/" do
        send_resp(conn, 200, "hello")
      end

      sync "/shapes/todos", Support.Todo
      sync "/shapes/things-to-do", table: "todos"

      sync "/shapes/ideas",
        table: "todos",
        where: "plausible = true",
        columns: ["id", "title"],
        replica: :full,
        storage: %{compaction: :disabled}

      sync "/shapes/query-module", Support.Todo, where: "completed = false"

      forward "/namespace", to: MyScope

      match _ do
        send_resp(conn, 404, "not found")
      end
    end

    setup(ctx) do
      [plug_opts: [phoenix_sync: Phoenix.Sync.plug_opts(ctx.electric_opts)]]
    end

    test "raises compile-time error if Plug.Router is not configured to copy_opts_to_assign" do
      assert_raise ArgumentError, fn ->
        Code.compile_string("""
        defmodule BreakingRouter#{System.unique_integer([:positive, :monotonic])} do
          use Plug.Router
          use Phoenix.Sync.Router

          plug :match
          plug :dispatch

          sync "/shapes/todos", Support.Todo
        end
        """)
      end
    end

    test "doesn't raise compile time error if copy_opts_to_assign is set in the opts" do
      Code.compile_string("""
      defmodule WorkingRouter#{System.unique_integer([:positive, :monotonic])} do
        use Plug.Router
        use Phoenix.Sync.Router, opts_in_assign: :options

        plug :match
        plug :dispatch

        sync "/todos", Support.Todo
      end
      """)
    end

    for path <-
          ~w(/shapes/todos /shapes/things-to-do /shapes/ideas /shapes/query-module /namespace/todos) do
      test "plug route #{path}", ctx do
        resp =
          conn(:get, unquote(path), %{"offset" => "-1"})
          |> MyRouter.call(ctx.plug_opts)

        assert resp.status == 200
        assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

        assert [
                 %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "one"}},
                 %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "two"}},
                 %{"headers" => %{"operation" => "insert"}, "value" => %{"title" => "three"}}
               ] = Jason.decode!(resp.resp_body)
      end
    end
  end
end
