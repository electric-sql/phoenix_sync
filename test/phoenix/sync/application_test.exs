defmodule Phoenix.Sync.ApplicationTest do
  use ExUnit.Case, async: false

  alias Phoenix.Sync.Application, as: App

  import ExUnit.CaptureLog

  Code.ensure_loaded!(Support.ConfigTestRepo)

  # Helper to validate storage configuration - handles both old and new formats
  # Electric 1.2.x may use different storage option formats
  defp validate_storage!(opts, expected_module, expected_path_prefix) do
    {module, storage_opts} = opts[:storage]
    assert module == expected_module

    # Handle both keyword list and map formats for storage options
    storage_path =
      cond do
        is_list(storage_opts) -> Keyword.get(storage_opts, :storage_dir)
        is_map(storage_opts) -> Map.get(storage_opts, :base_path) || Map.get(storage_opts, :storage_dir)
        true -> nil
      end

    assert storage_path != nil, "Storage path not found in #{inspect(storage_opts)}"
    assert String.starts_with?(storage_path, expected_path_prefix),
           "Expected storage path to start with #{expected_path_prefix}, got #{storage_path}"
  end

  defp validate_repo_connection_opts!(opts, overrides \\ []) do
    for connection_opts <- [
          get_in(opts, [:replication_opts, :connection_opts]) || [],
          opts[:connection_opts]
        ] do
      assert {"password", connection_opts} =
               Keyword.pop!(connection_opts, :password)

      base_opts = [
        username: "postgres",
        hostname: "localhost",
        database: "phoenix_sync",
        port: 5432,
        sslmode: :disable
      ]

      expected_opts = Keyword.merge(base_opts, overrides)

      assert Enum.sort(connection_opts) == Enum.sort(expected_opts)
    end
  end

  defp with_modified_config(repo_config_overrides, fun) do
    original_config = Application.get_env(:phoenix_sync, Support.ConfigTestRepo, [])

    try do
      Application.put_env(
        :phoenix_sync,
        Support.ConfigTestRepo,
        Keyword.merge(original_config, repo_config_overrides)
      )

      fun.()
    after
      Application.put_env(
        :phoenix_sync,
        Support.ConfigTestRepo,
        original_config
      )
    end
  end

  describe "children/1" do
    test "invalid mode" do
      assert {:error, _} = App.children(mode: :nonsense)
      assert {:error, _} = App.children([])
    end

    test "embedded mode" do
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :prod,
        repo: Support.ConfigTestRepo,
        storage_dir: storage_dir
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)
      opts_map = Map.new(opts)

      validate_storage!(opts_map, Electric.ShapeCache.PureFileStorage, storage_dir)
      assert %{persistent_kv: %Electric.PersistentKV.Filesystem{root: ^storage_dir}} = opts_map
    end

    test "no configuration set" do
      assert {:error, _} = App.children([])
    end

    test "disabled mode" do
      refute capture_log(fn ->
               assert {:ok, []} = App.children(mode: :disabled)
             end) =~ ~r/No `env` specified for :phoenix_sync: defaulting to `:prod`/
    end

    test "warns if env not set" do
      config = [
        mode: :embedded,
        repo: Support.ConfigTestRepo
      ]

      assert capture_log(fn ->
               assert {:ok, _} = App.children(config)
             end) =~ ~r/No `env` specified for :phoenix_sync: defaulting to `:prod`/
    end

    test "sandbox mode" do
      config = [
        mode: :sandbox,
        repo: Support.ConfigTestRepo
      ]

      assert {:ok, [Phoenix.Sync.Sandbox]} = App.children(config)
    end

    test "embedded mode dev env" do
      tmp_dir = System.tmp_dir!()

      config = [
        mode: :embedded,
        env: :dev,
        repo: Support.ConfigTestRepo
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)
      opts_map = Map.new(opts)

      # Dev env uses tmp_dir with phoenix-sync prefix
      validate_storage!(opts_map, Electric.ShapeCache.PureFileStorage, tmp_dir)
      assert %{persistent_kv: %Electric.PersistentKV.Filesystem{root: root}} = opts_map
      assert String.starts_with?(root, tmp_dir)
    end

    test "passes repo pg port to electric" do
      config = [
        env: :dev,
        repo: Support.ConfigTestRepo
      ]

      repo_override = [port: 6543]

      with_modified_config(repo_override, fn ->
        assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

        validate_repo_connection_opts!(opts, repo_override)
      end)
    end

    test "maps repo ssl and ipv6 settings to electric" do
      config = [
        env: :dev,
        repo: Support.ConfigTestRepo
      ]

      repo_override = [
        ssl: true,
        socket_options: [:inet6]
      ]

      with_modified_config(repo_override, fn ->
        assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

        validate_repo_connection_opts!(opts,
          sslmode: :require,
          ipv6: true
        )
      end)
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
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :dev,
        repo: Support.ConfigTestRepo,
        # don't overwrite this explicit config
        storage_dir: storage_dir
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      validate_repo_connection_opts!(opts)
      opts_map = Map.new(opts)

      validate_storage!(opts_map, Electric.ShapeCache.PureFileStorage, storage_dir)
      assert %{persistent_kv: %Electric.PersistentKV.Filesystem{root: ^storage_dir}} = opts_map
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

    test "embedded mode with explicit replication_connection_opts and query_connection_opts" do
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :prod,
        replication_connection_opts: [
          username: "postgres",
          hostname: "localhost",
          database: "phoenix_sync",
          password: "password"
        ],
        query_connection_opts: [
          username: "postgres",
          hostname: "localhost-pooled",
          database: "phoenix_sync",
          password: "password"
        ],
        storage_dir: storage_dir
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      assert {"password", connection_opts} =
               Keyword.pop!(opts[:replication_opts][:connection_opts], :password)

      assert connection_opts == [
               username: "postgres",
               hostname: "localhost",
               database: "phoenix_sync"
             ]

      assert {"password", connection_opts} =
               Keyword.pop!(opts[:connection_opts], :password)

      assert connection_opts == [
               username: "postgres",
               hostname: "localhost-pooled",
               database: "phoenix_sync"
             ]

      opts_map = Map.new(opts)
      validate_storage!(opts_map, Electric.ShapeCache.PureFileStorage, storage_dir)
      assert %{persistent_kv: %Electric.PersistentKV.Filesystem{root: ^storage_dir}} = opts_map
    end

    test "embedded mode with explicit connection_opts" do
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :prod,
        connection_opts: [
          username: "postgres",
          hostname: "localhost",
          database: "phoenix_sync",
          password: "password"
        ],
        storage_dir: storage_dir
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      assert {"password", connection_opts} = Keyword.pop!(opts[:connection_opts], :password)

      assert connection_opts == [
               username: "postgres",
               hostname: "localhost",
               database: "phoenix_sync"
             ]

      opts_map = Map.new(opts)
      validate_storage!(opts_map, Electric.ShapeCache.PureFileStorage, storage_dir)
      assert %{persistent_kv: %Electric.PersistentKV.Filesystem{root: ^storage_dir}} = opts_map
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
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :dev,
        repo: Support.ConfigTestRepo,
        storage_dir: storage_dir
      ]

      api = App.plug_opts(config)

      assert %Electric.Shapes.Api{
               storage: {Electric.ShapeCache.PureFileStorage, storage_opts},
               persistent_kv: %Electric.PersistentKV.Filesystem{root: ^storage_dir}
             } = api

      # Electric.Shapes.Api returns processed storage options as a map
      storage_path = Map.get(storage_opts, :base_path) || Map.get(storage_opts, :storage_dir)
      assert storage_path != nil
      assert String.starts_with?(storage_path, storage_dir)
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

    test "sandbox mode" do
      config = [
        mode: :sandbox,
        repo: Support.ConfigTestRepo
      ]

      api = App.plug_opts(config)

      assert %Phoenix.Sync.Sandbox.APIAdapter{} = api
    end
  end

  describe "Electric 1.2.x compatibility" do
    test "passes live_sse configuration to Electric" do
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :prod,
        repo: Support.ConfigTestRepo,
        storage_dir: storage_dir,
        # live_sse replaces experimental_live_sse in Electric 1.2.x
        live_sse: true
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      # Verify the option is passed through to Electric configuration
      assert Keyword.get(opts, :live_sse) == true
    end

    test "passes max_shapes configuration to Electric" do
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :prod,
        repo: Support.ConfigTestRepo,
        storage_dir: storage_dir,
        max_shapes: 100
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      # Verify max_shapes is passed through
      assert Keyword.get(opts, :max_shapes) == 100
    end

    test "passes replication_idle_timeout configuration to Electric" do
      storage_dir = Path.join([System.tmp_dir!(), "storage-dir#{System.monotonic_time()}"])

      config = [
        mode: :embedded,
        env: :prod,
        repo: Support.ConfigTestRepo,
        storage_dir: storage_dir,
        # New in Electric 1.2.x for scale-to-zero deployments
        replication_idle_timeout: 30_000
      ]

      assert {:ok, [{Electric.StackSupervisor, opts}]} = App.children(config)

      # Verify the option is passed through
      assert Keyword.get(opts, :replication_idle_timeout) == 30_000
    end
  end
end
