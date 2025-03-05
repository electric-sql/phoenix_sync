defmodule Phoenix.Sync.ElectricTest do
  use ExUnit.Case,
    async: false,
    parameterize: [
      %{
        sync_config: [
          mode: :embedded,
          pool_opts: [backoff_type: :stop, max_restarts: 0, pool_size: 2]
        ]
      },
      %{
        sync_config: [
          mode: :http,
          url: "http://localhost:3000",
          pool_opts: [backoff_type: :stop, max_restarts: 0, pool_size: 2]
        ]
      }
    ]

  use Plug.Test
  use Support.ElectricHelpers, endpoint: __MODULE__.Endpoint

  import Mox

  require Phoenix.ConnTest

  defmodule Router do
    use Phoenix.Router

    pipeline :browser do
      plug :accepts, ["html"]
    end

    scope "/api" do
      pipe_through [:browser]

      forward "/", Phoenix.Sync.Electric
    end
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :phoenix_sync

    plug Router
  end

  Code.ensure_loaded!(Support.Todo)
  Code.ensure_loaded!(Support.Repo)

  defmodule MyEnv do
    def client!(opts \\ []) do
      Electric.Client.new!(
        base_url: "https://cloud.electric-sql.com",
        authenticator:
          Keyword.get(
            opts,
            :authenticator,
            {Electric.Client.Authenticator.MockAuthenticator, salt: "my-salt"}
          )
      )
    end

    def authenticate(conn, shape, opts \\ [])

    def authenticate(%Plug.Conn{} = conn, %Electric.Client.ShapeDefinition{} = shape, opts) do
      mode = Keyword.get(opts, :mode, :fun)

      %{
        "shape-auth-mode" => to_string(mode),
        "shape-auth-path" => conn.request_path,
        "shape-auth-table" => shape.table
      }
    end
  end

  setup :verify_on_exit!

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

  defmodule MyEnv.TestRouter do
    use Plug.Router, copy_opts_to_assign: :config
    use Phoenix.Sync.Electric

    plug :match
    plug :dispatch

    forward "/shapes",
      to: Phoenix.Sync.Electric,
      init_opts: [opts_in_assign: :config]
  end

  defp call(conn, plug \\ MyEnv.TestRouter, ctx) do
    opts = Phoenix.Sync.plug_opts(electric_opts(ctx))

    plug.call(conn, phoenix_sync: opts)
  end

  describe "Plug" do
    @describetag table: {
                   "things",
                   ["id int8 not null primary key generated always as identity", "value text"]
                 }
    @describetag data: {"things", ["value"], [["one"], ["two"], ["three"]]}

    test "provides the standard electric http api", ctx do
      resp =
        conn(:get, "/shapes", %{"table" => "things", "offset" => "-1"})
        |> call(ctx)

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"value" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"value" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"value" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "supports DELETEs", ctx do
      resp =
        conn(:get, "/shapes", %{"table" => "things", "offset" => "-1"})
        |> call(ctx)

      assert resp.status == 200
      [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      resp =
        conn(:delete, "/shapes?handle=#{handle}")
        |> call(ctx)

      # api is not configured to allow deletes
      assert resp.status == 405

      resp =
        conn(:delete, "/shapes?handle=#{handle}")
        |> call(Map.put(ctx, :allow_shape_deletion, true))

      assert resp.status == 202
    end

    test "supports OPTIONS", ctx do
      resp =
        conn(:options, "/shapes", %{"table" => "things", "offset" => "-1"})
        |> Plug.Conn.put_req_header("access-control-request-headers", "if-none-match")
        |> call(ctx)

      assert resp.status == 204

      assert ["if-none-match"] = Plug.Conn.get_resp_header(resp, "access-control-allow-headers")
    end
  end

  describe "Phoenix" do
    @describetag table: {
                   "things",
                   ["id int8 not null primary key generated always as identity", "value text"]
                 }
    @describetag data: {"things", ["value"], [["one"], ["two"], ["three"]]}

    test "provides the full shape api", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/api", %{table: "things", offset: "-1"})

      assert resp.status == 200
      assert Plug.Conn.get_resp_header(resp, "electric-offset") == ["0_0"]

      assert [
               %{"headers" => %{"operation" => "insert"}, "value" => %{"value" => "one"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"value" => "two"}},
               %{"headers" => %{"operation" => "insert"}, "value" => %{"value" => "three"}}
             ] = Jason.decode!(resp.resp_body)
    end

    test "supports deletes", _ctx do
      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.get("/api", %{table: "things", offset: "-1"})

      assert resp.status == 200
      assert [handle] = Plug.Conn.get_resp_header(resp, "electric-handle")

      resp =
        Phoenix.ConnTest.build_conn()
        |> Phoenix.ConnTest.delete("/api", %{handle: handle})

      # method not allowed -- specific to the delete plug...
      assert resp.status == 405
    end
  end
end
