defmodule Phoenix.Sync.ClientTest do
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
          env: :test,
          mode: :http,
          url: "http://localhost:3000",
          pool_opts: [backoff_type: :stop, max_restarts: 0, pool_size: 2]
        ]
      }
    ]

  alias Phoenix.Sync.Client
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.Message.Headers
  alias Electric.Client.Message.ControlMessage

  import Support.DbSetup
  import Support.ElectricHelpers

  import Ecto.Query, only: [from: 2]

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

  defp assert_embedded_client(client) do
    assert %Electric.Client{fetch: {Electric.Client.Embedded, _}} = client
  end

  defp assert_http_client(client, endpoint) do
    endpoint = URI.new!(endpoint)
    assert %Electric.Client{endpoint: ^endpoint, fetch: {Electric.Client.Fetch.HTTP, _}} = client
  end

  setup [
    :with_stack_id_from_test,
    :with_unique_db,
    :with_stack_config,
    :with_table,
    :with_data,
    :start_embedded
  ]

  describe "client/1" do
    test "returns embedded client when configured" do
      config = [
        mode: :embedded
      ]

      assert {:ok, client} = Client.new(config)

      assert_embedded_client(client)
    end

    test "returns http client when configured" do
      config = [
        mode: :http,
        url: "http://api.electric-sql.cloud"
      ]

      assert {:ok, client} = Client.new(config)

      assert_http_client(client, "http://api.electric-sql.cloud/v1/shape")
    end

    test "passes credentials into client" do
      config = [
        mode: :http,
        url: "http://api.electric-sql.cloud",
        credentials: [
          secret: "my-secret",
          source_id: "my-source-id"
        ],
        params: %{
          something: "here"
        }
      ]

      assert {:ok, client} = Client.new(config)

      assert client.params == %{secret: "my-secret", source_id: "my-source-id", something: "here"}
    end
  end

  describe "stream" do
    test "with schema module", ctx do
      stream = Phoenix.Sync.Client.stream(Support.Todo, [], ctx.electric_opts)

      events = Enum.take(stream, 4)

      assert [
               %ChangeMessage{
                 value: %Support.Todo{title: "one", completed: false},
                 headers: %Headers{operation: :insert}
               },
               %ChangeMessage{
                 value: %Support.Todo{title: "two", completed: false},
                 headers: %Headers{operation: :insert}
               },
               %ChangeMessage{
                 value: %Support.Todo{title: "three", completed: true},
                 headers: %Headers{operation: :insert}
               },
               %ControlMessage{control: :up_to_date}
             ] = events
    end

    test "with ecto query", ctx do
      stream =
        Phoenix.Sync.Client.stream(
          from(t in Support.Todo, where: t.completed == true),
          [],
          ctx.electric_opts
        )

      events = Enum.take(stream, 2)

      assert [
               %ChangeMessage{
                 value: %Support.Todo{title: "three", completed: true},
                 headers: %Headers{operation: :insert}
               },
               %ControlMessage{control: :up_to_date}
             ] = events
    end

    test "with table name", ctx do
      stream =
        Phoenix.Sync.Client.stream(
          "todos",
          [],
          ctx.electric_opts
        )

      events = Enum.take(stream, 4)

      assert [
               %ChangeMessage{
                 value: %{"title" => "one", "completed" => "false"},
                 headers: %Headers{operation: :insert}
               },
               %ChangeMessage{
                 value: %{"title" => "two", "completed" => "false"},
                 headers: %Headers{operation: :insert}
               },
               %ChangeMessage{
                 value: %{"title" => "three", "completed" => "true"},
                 headers: %Headers{operation: :insert}
               },
               %ControlMessage{control: :up_to_date}
             ] = events
    end

    test "with shape params", ctx do
      stream =
        Phoenix.Sync.Client.stream(
          [
            table: "todos",
            namespace: "public",
            where: "completed = true",
            columns: ["id", "title"]
          ],
          [],
          ctx.electric_opts
        )

      events = Enum.take(stream, 2)

      assert [
               %ChangeMessage{
                 value: %{"title" => "three"},
                 headers: %Headers{operation: :insert}
               },
               %ControlMessage{control: :up_to_date}
             ] = events
    end
  end
end
