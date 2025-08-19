defmodule Mix.Tasks.PhoenixSync.InstallTest do
  use ExUnit.Case, async: true

  import Igniter.Test

  defp run_install_task(igniter, ctx) do
    install_args = Map.fetch!(ctx, :install_args)

    igniter
    |> Igniter.compose_task("phoenix_sync.install", install_args)
  end

  describe "phoenix: embedded" do
    @describetag install_args: ["--sync-mode", "embedded"]

    setup(ctx) do
      files = Map.get(ctx, :files, %{})

      [igniter: run_install_task(phx_test_project(files: files), ctx)]
    end

    test "adds electric dependency", ctx do
      ctx.igniter
      |> assert_has_patch(
        "mix.exs",
        """
        + |      {:electric, "#{Phoenix.Sync.MixProject.electric_version()}"}
        """
      )
    end

    test "updates existing config files", ctx do
      ctx.igniter
      |> assert_has_patch(
        "config/config.exs",
        """
        + |config :phoenix_sync, mode: :embedded, env: config_env(), repo: Test.Repo
        """
      )
      |> assert_has_patch(
        "config/test.exs",
        """
        + |config :phoenix_sync, mode: :sandbox, env: config_env()
        """
      )
    end

    test "configures the endpoint", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test/application.ex",
        """
        - |      TestWeb.Endpoint
        + |      {TestWeb.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
        """
      )
    end

    test "appends sync config to any existing params", ctx do
      phx_test_project()
      |> Igniter.update_file("lib/test/application.ex", fn source ->
        Rewrite.Source.update(source, :content, fn content ->
          String.replace(
            content,
            ~r/TestWeb\.Endpoint/,
            "{TestWeb.Endpoint, config: [here: true]}"
          )
        end)
      end)
      |> run_install_task(ctx)
      |> assert_has_patch(
        "lib/test/application.ex",
        # the final patch is my source update above plus the effect of the install
        """
        - |      TestWeb.Endpoint
        + |      {TestWeb.Endpoint, config: [here: true], phoenix_sync: Phoenix.Sync.plug_opts()}
        """
      )
    end

    test "adds the sandbox config to the repo", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test/repo.ex",
        """
        + |  use Phoenix.Sync.Sandbox.Postgres
        + |
        """
      )
      |> assert_has_patch(
        "lib/test/repo.ex",
        """
        - |    adapter: Ecto.Adapters.Postgres
        + |    adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
        """
      )
    end

    @tag install_args: ["--sync-mode", "embedded", "--no-sync-sandbox"]
    test "doesn't add the sandbox config if passed --no-sync-sandbox", ctx do
      ctx.igniter
      |> assert_unchanged("lib/test/repo.ex")
    end

    test "adds connection_opts to repo-less application", ctx do
      phx_test_project()
      |> Igniter.rm("lib/test/repo.ex")
      |> run_install_task(ctx)
      |> assert_has_notice(fn notice ->
        assert notice =~ "No Ecto.Repo found"
      end)
      |> assert_has_patch(
        "config/config.exs",
        """
        + |# add your real database connection details
        + |config :phoenix_sync,
        + |  mode: :embedded,
        + |  env: config_env(),
        + |  connection_opts: [
        + |    username: "your_username",
        + |    password: "your_password",
        + |    hostname: "localhost",
        + |    database: "your_database",
        + |    port: 5432,
        + |    sslmode: :disable
        + |  ]
        """
      )
    end
  end

  describe "phoenix: http" do
    @describetag install_args: [
                   "--sync-mode",
                   "http",
                   "--sync-url",
                   "https://electric-sql.cloud/"
                 ]

    setup(ctx) do
      files = Map.get(ctx, :files, %{})

      [igniter: run_install_task(phx_test_project(files: files), ctx)]
    end

    @tag install_args: ["--sync-mode", "http"]
    test "returns issue if url is not provided", ctx do
      ctx.igniter
      |> assert_has_issue(fn issue ->
        assert issue =~ "`--sync-url` is required for :http mode"
      end)
    end

    test "adds url and empty credentials to config.exs", ctx do
      ctx.igniter
      |> assert_has_patch(
        "config/config.exs",
        """
        + |config :phoenix_sync,
        + |  mode: :http,
        + |  env: config_env(),
        + |  url: "https://electric-sql.cloud/",
        + |  credentials: [secret: "MY_SECRET", source_id: "00000000-0000-0000-0000-000000000000"]
        + |
        """
      )
    end

    test "configures the endpoint", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test/application.ex",
        """
        - |      TestWeb.Endpoint
        + |      {TestWeb.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
        """
      )
    end
  end

  plug_application = fn server_spec ->
    """
    defmodule TestPlug.Application do
      use Application

      def start(_type, _args) do
        children = [
          #{server_spec}
        ]

        opts = [strategy: :one_for_one, name: TestPlug.Supervisor]
        Supervisor.start_link(children, opts)
      end
    end
    """
  end

  @plug_files %{
    "mix.exs" => """
    defmodule TestPlug.MixProject do
      use Mix.Project

      def project do
        [
          app: :test_plug,
          version: "0.1.0",
          elixir: "~> 1.17",
          start_permanent: Mix.env() == :prod,
          deps: deps()
        ]
      end

      # Run "mix help compile.app" to learn about applications.
      def application do
        [
          extra_applications: [:logger],
          mod: {TestPlug.Application, []}
        ]
      end

      # Run "mix help deps" to learn about dependencies.
      defp deps do
        []
      end
    end
    """,
    "lib/test_plug/application.ex" =>
      plug_application.(
        "{Plug.Cowboy, scheme: :http, plug: PlugTest.Router, options: [port: 4040]}"
      ),
    "lib/test_plug/repo.ex" => """
    defmodule TestPlug.Repo do
      use Ecto.Repo,
        otp_app: :test_plug,
        adapter: Ecto.Adapters.Postgres
    end
    """
  }
  describe "Plug: embedded" do
    @describetag install_args: ["--sync-mode", "embedded"]

    setup(ctx) do
      extra_files = Map.get(ctx, :files, %{})

      files = Map.merge(@plug_files, extra_files)

      [igniter: run_install_task(test_project(files: files), ctx)]
    end

    test "adds electric dependency", ctx do
      ctx.igniter
      |> assert_has_patch(
        "mix.exs",
        """
        - |      []
        + |      [{:electric, "#{Phoenix.Sync.MixProject.electric_version()}"}]
        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.(
               "{Plug.Cowboy, scheme: :http, plug: TestPlug.Router, options: [port: 4040]}"
             )
         }
    test "configures Plug.Cowboy app", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Plug.Cowboy, scheme: :http, plug: TestPlug.Router, options: [port: 4040]}
        + |      {Plug.Cowboy,
        + |       scheme: :http,
        + |       plug: {TestPlug.Router, phoenix_sync: Phoenix.Sync.plug_opts()},
        + |       options: [port: 4040]}

        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.(
               "{Plug.Cowboy, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, options: [port: 4040]}"
             )
         }
    test "configures Plug.Cowboy app which already has config", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Plug.Cowboy, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, options: [port: 4040]}
        + |      {Plug.Cowboy,
        + |       scheme: :http,
        + |       plug: {TestPlug.Router, [my_config: [here: true], phoenix_sync: Phoenix.Sync.plug_opts()]},
        + |       options: [port: 4040]}

        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.("{Bandit, scheme: :http, plug: TestPlug.Router, port: 4040}")
         }
    test "configures Bandit app", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Bandit, scheme: :http, plug: TestPlug.Router, port: 4040}
        + |      {Bandit,
        + |       scheme: :http, plug: {TestPlug.Router, phoenix_sync: Phoenix.Sync.plug_opts()}, port: 4040}
        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.(
               "{Bandit, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, port: 4040}"
             )
         }
    test "configures Bandit app with existing config", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Bandit, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, port: 4040}
        + |      {Bandit,
        + |       scheme: :http,
        + |       plug: {TestPlug.Router, [my_config: [here: true], phoenix_sync: Phoenix.Sync.plug_opts()]},
        + |       port: 4040}

        """
      )
    end

    @tag files: %{
           "config/config.exs" => """
           import Config

           import_config "\#{config_env()}.exs"
           """,
           "config/test.exs" => """
           import Config
           """
         }
    test "updates existing config files", ctx do
      ctx.igniter
      |> assert_has_patch(
        "config/config.exs",
        """
        + |config :phoenix_sync, mode: :embedded, env: config_env(), repo: TestPlug.Repo
        """
      )
      |> assert_has_patch(
        "config/test.exs",
        """
        + |config :phoenix_sync, mode: :sandbox, env: config_env()
        """
      )
    end

    test "creates config files", ctx do
      ctx.igniter
      |> assert_creates(
        "config/config.exs",
        """
        import Config
        config :phoenix_sync, mode: :embedded, env: config_env(), repo: TestPlug.Repo
        import_config "\#{config_env()}.exs"
        """
      )
      |> assert_creates(
        "config/test.exs",
        """
        import Config
        config :phoenix_sync, mode: :sandbox, env: config_env()
        """
      )
    end

    test "adds the sandbox config to the repo", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/repo.ex",
        """
        + |  use Phoenix.Sync.Sandbox.Postgres
        + |
        """
      )
      |> assert_has_patch(
        "lib/test_plug/repo.ex",
        """
        - |    adapter: Ecto.Adapters.Postgres
        + |    adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
        """
      )
    end

    @tag install_args: ["--sync-mode", "embedded", "--no-sync-sandbox"]
    test "honours --no-sync-sandbox", ctx do
      ctx.igniter
      |> assert_unchanged("lib/test_plug/repo.ex")
    end

    @tag files: %{"lib/test_plug/repo.ex" => ""}
    test "handles a repo-less application", ctx do
      ctx.igniter
      |> assert_unchanged("lib/test_plug/repo.ex")
      |> assert_creates(
        "config/config.exs",
        """
        import Config

        config :phoenix_sync,
          mode: :embedded,
          env: config_env(),
          # add your real database connection details
          connection_opts: [
            username: "your_username",
            password: "your_password",
            hostname: "localhost",
            database: "your_database",
            port: 5432,
            sslmode: :disable
          ]
        """
      )
      |> refute_creates("config/test.exs")
    end
  end

  describe "Plug: http" do
    @describetag install_args: [
                   "--sync-mode",
                   "http",
                   "--sync-url",
                   "https://electric-sql.cloud/"
                 ]

    setup(ctx) do
      extra_files = Map.get(ctx, :files, %{})

      files = Map.put(Map.merge(@plug_files, extra_files), "lib/test_plug/repo.ex", "")

      [igniter: run_install_task(test_project(files: files), ctx)]
    end

    test "adds electric dependency", ctx do
      ctx.igniter
      |> assert_unchanged("mix.exs")
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.(
               "{Plug.Cowboy, scheme: :http, plug: TestPlug.Router, options: [port: 4040]}"
             )
         }
    test "configures Plug.Cowboy app", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Plug.Cowboy, scheme: :http, plug: TestPlug.Router, options: [port: 4040]}
        + |      {Plug.Cowboy,
        + |       scheme: :http,
        + |       plug: {TestPlug.Router, phoenix_sync: Phoenix.Sync.plug_opts()},
        + |       options: [port: 4040]}

        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.(
               "{Plug.Cowboy, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, options: [port: 4040]}"
             )
         }
    test "configures Plug.Cowboy app which already has config", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Plug.Cowboy, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, options: [port: 4040]}
        + |      {Plug.Cowboy,
        + |       scheme: :http,
        + |       plug: {TestPlug.Router, [my_config: [here: true], phoenix_sync: Phoenix.Sync.plug_opts()]},
        + |       options: [port: 4040]}

        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" => """
           defmodule TestPlug.Application do
             use Application

             alias TestPlug.Router

             def start(_type, _args) do
               children = [
                 {Plug.Cowboy, scheme: :http, plug: {Router, my_config: [here: true]}, options: [port: 4040]}
               ]

               opts = [strategy: :one_for_one, name: TestPlug.Supervisor]
               Supervisor.start_link(children, opts)
             end
           end
           """
         }
    test "supports aliased plug modules", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Plug.Cowboy, scheme: :http, plug: {Router, my_config: [here: true]}, options: [port: 4040]}
        + |      {Plug.Cowboy,
        + |       scheme: :http,
        + |       plug: {Router, [my_config: [here: true], phoenix_sync: Phoenix.Sync.plug_opts()]},
        + |       options: [port: 4040]}

        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.("{Bandit, scheme: :http, plug: TestPlug.Router, port: 4040}")
         }
    test "configures Bandit app", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Bandit, scheme: :http, plug: TestPlug.Router, port: 4040}
        + |      {Bandit,
        + |       scheme: :http, plug: {TestPlug.Router, phoenix_sync: Phoenix.Sync.plug_opts()}, port: 4040}
        """
      )
    end

    @tag files: %{
           "lib/test_plug/application.ex" =>
             plug_application.(
               "{Bandit, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, port: 4040}"
             )
         }
    test "configures Bandit app with existing config", ctx do
      ctx.igniter
      |> assert_has_patch(
        "lib/test_plug/application.ex",
        """
        - |      {Bandit, scheme: :http, plug: {TestPlug.Router, my_config: [here: true]}, port: 4040}
        + |      {Bandit,
        + |       scheme: :http,
        + |       plug: {TestPlug.Router, [my_config: [here: true], phoenix_sync: Phoenix.Sync.plug_opts()]},
        + |       port: 4040}

        """
      )
    end

    @tag files: %{
           "config/config.exs" => """
           import Config

           import_config "\#{config_env()}.exs"
           """,
           "config/test.exs" => """
           import Config
           """
         }
    test "updates existing config files", ctx do
      ctx.igniter
      |> assert_has_patch(
        "config/config.exs",
        """
        + |config :phoenix_sync,
        + |  mode: :http,
        + |  env: config_env(),
        + |  url: "https://electric-sql.cloud/",
        + |  credentials: [secret: "MY_SECRET", source_id: "00000000-0000-0000-0000-000000000000"]
        + |
        """
      )
      |> assert_unchanged("config/test.exs")
    end

    test "creates config files", ctx do
      ctx.igniter
      |> assert_creates(
        "config/config.exs",
        """
        import Config

        config :phoenix_sync,
          mode: :http,
          env: config_env(),
          url: "https://electric-sql.cloud/",
          credentials: [secret: "MY_SECRET", source_id: "00000000-0000-0000-0000-000000000000"]
        """
      )
      |> refute_creates("config/test.exs")
    end
  end
end
