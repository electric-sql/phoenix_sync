defmodule Phoenix.Sync.ApplicationTest do
  use ExUnit.Case, async: false

  alias Phoenix.Sync.Application, as: App

  Code.ensure_loaded!(Support.ConfigTestRepo)

  defp validate_repo_connection_opts!(opts) do
    assert {pass_fun, connection_opts} = Keyword.pop!(opts[:connection_opts], :password)

    assert pass_fun.() == "password"

    assert connection_opts == [
             username: "postgres",
             hostname: "localhost",
             database: "electric",
             port: 54321,
             sslmode: :require,
             ipv6: true
           ]
  end

  describe "children/1" do
    test "invalid mode" do
      assert {:error, _} = App.children(mode: :nonsense)
      assert {:error, _} = App.children([])
    end

    test "embedded mode" do
      config = [
        mode: :embedded,
        env: :prod,
        repo: Support.ConfigTestRepo,
        storage_dir: "/something"
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)

      assert %{
               storage: {Electric.ShapeCache.FileStorage, [storage_dir: "/something"]},
               persistent_kv: %Electric.PersistentKV.Filesystem{root: "/something"}
             } = Map.new(opts)
    end

    test "no configuration set" do
      assert {:error, _} = App.children([])
    end

    test "disabled mode" do
      assert {:ok, []} = App.children(mode: :disabled)
    end

    test "embedded mode dev env" do
      config = [
        mode: :embedded,
        env: :dev,
        repo: Support.ConfigTestRepo
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)

      assert %{
               storage: {Electric.ShapeCache.FileStorage, [storage_dir: "/tmp/" <> storage_dir]},
               persistent_kv: %Electric.PersistentKV.Filesystem{
                 root: "/tmp/" <> storage_dir
               }
             } = Map.new(opts)
    end

    test "only repo config given and electric installed defaults to embedded" do
      config = [
        env: :dev,
        repo: Support.ConfigTestRepo
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)
      validate_repo_connection_opts!(opts)
    end

    test "embedded mode dev env doesn't overwrite explicit storage_dir" do
      config = [
        mode: :embedded,
        env: :dev,
        repo: Support.ConfigTestRepo,
        # don't overwrite this explict config
        storage_dir: "/something"
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)

      assert %{
               storage: {Electric.ShapeCache.FileStorage, [storage_dir: "/something"]},
               persistent_kv: %Electric.PersistentKV.Filesystem{root: "/something"}
             } = Map.new(opts)
    end

    test "embedded mode test env" do
      config = [
        mode: :embedded,
        env: :test,
        repo: Support.ConfigTestRepo,
        storage_dir: "/something"
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)

      assert %{
               storage: {Electric.ShapeCache.InMemoryStorage, _},
               persistent_kv: %Electric.PersistentKV.Memory{}
             } = Map.new(opts)
    end

    test "embedded mode with explict connection_opts" do
      config = [
        mode: :embedded,
        env: :prod,
        connection_opts: [
          username: "postgres",
          hostname: "localhost",
          database: "electric",
          password: "password"
        ],
        storage_dir: "/something"
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      assert {pass_fun, connection_opts} = Keyword.pop!(opts[:connection_opts], :password)
      assert pass_fun.() == "password"

      assert connection_opts == [
               username: "postgres",
               hostname: "localhost",
               database: "electric"
             ]

      assert %{
               storage: {Electric.ShapeCache.FileStorage, [storage_dir: "/something"]},
               persistent_kv: %Electric.PersistentKV.Filesystem{root: "/something"}
             } = Map.new(opts)
    end

    test "remote http mode" do
      config = [
        mode: :http,
        env: :prod,
        url: "https://api.electric-sql.cloud",
        credentials: [
          secret: "my-secret",
          source_id: "my-source-id"
        ]
      ]

      assert {:ok, []} = App.children(config)
    end

    test "embedded http mode" do
      config = [
        mode: :http,
        env: :prod,
        repo: Support.ConfigTestRepo,
        url: "http://localhost:4001",
        http: [
          ip: :loopback,
          port: 4001
        ]
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}, {Bandit, http_opts}]} =
               App.children(config)

      validate_repo_connection_opts!(opts)

      assert {Electric.Plug.Router, _} = http_opts[:plug]
      assert http_opts[:port] == 4001
    end
  end

  describe "plug_opts/1" do
    test "embedded mode" do
      config = [
        mode: :embedded,
        env: :dev,
        repo: Support.ConfigTestRepo,
        storage_dir: "/something"
      ]

      api = App.plug_opts(config)

      assert %Electric.Shapes.Api{
               storage: {Electric.ShapeCache.FileStorage, %{base_path: "/something" <> _}},
               persistent_kv: %Electric.PersistentKV.Filesystem{root: "/something"}
             } = api
    end

    test "remote http mode" do
      url = "https://api.electric-sql.cloud"

      config = [
        mode: :http,
        env: :prod,
        url: url,
        credentials: [
          secret: "my-secret",
          source_id: "my-source-id"
        ],
        params: %{
          something: "here"
        }
      ]

      endpoint = URI.new!(url) |> URI.append_path("/v1/shape")

      assert api = App.plug_opts(config)

      assert %Phoenix.Sync.Electric.ClientAdapter{
               client: %Electric.Client{
                 endpoint: ^endpoint,
                 params: %{secret: "my-secret", source_id: "my-source-id", something: "here"}
               }
             } = api
    end

    test "embedded http mode" do
      url = "http://localhost:4000"

      config = [
        mode: :http,
        env: :prod,
        repo: Support.ConfigTestRepo,
        url: "http://localhost:4000",
        http: [
          ip: :loopback,
          port: 4000
        ]
      ]

      endpoint = URI.new!(url) |> URI.append_path("/v1/shape")

      assert api = App.plug_opts(config)

      assert %Phoenix.Sync.Electric.ClientAdapter{
               client: %Electric.Client{
                 endpoint: ^endpoint
               }
             } = api
    end
  end
end
