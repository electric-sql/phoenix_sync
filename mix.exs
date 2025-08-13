defmodule Phoenix.Sync.MixProject do
  use Mix.Project

  # Remember to update the README when you change the version
  @version "0.5.0"

  def project do
    [
      app: :phoenix_sync,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      consolidate_protocols: Mix.env() in [:dev, :prod],
      deps: deps(),
      name: "Phoenix.Sync",
      docs: docs(),
      package: package(),
      description: description(),
      source_url: "https://github.com/electric-sql/phoenix_sync",
      homepage_url: "https://hexdocs.pm/phoenix_sync",
      aliases: aliases(),
      preferred_cli_env: ["test.all": :test, "test.apps": :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Phoenix.Sync.Application, []}
    ]
  end

  def cli do
    [preferred_envs: ["test.all": :test, "test.apps": :test]]
  end

  defp deps do
    [
      {:nimble_options, "~> 1.1"},
      {:phoenix_live_view, "~> 1.0", optional: true},
      {:plug, "~> 1.0"},
      {:jason, "~> 1.0"},
      {:ecto_sql, "~> 3.10", optional: true},
      {:electric, "~> 1.0.24", optional: true},
      # 0.6.5 has the decoding and control message fix
      {:electric_client, ">= 0.6.5-beta-5"}
    ] ++ deps_for_env(Mix.env())
  end

  defp deps_for_env(:test) do
    [
      {:bandit, "~> 1.5", only: [:test], override: true},
      {:floki, "~> 0.36", only: [:test]},
      {:lazy_html, ">= 0.1.0", only: :test},
      {:mox, "~> 1.1", only: [:test]},
      {:uuid, "~> 1.1", only: [:test]}
    ]
  end

  defp deps_for_env(:dev) do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:makeup_ts, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp deps_for_env(_) do
    []
  end

  defp aliases do
    [
      "test.all": ["test", "test.as_a_dep", "test.apps"],
      "test.as_a_dep": ["test.as_a_dep.embedded", "test.as_a_dep.standalone"],
      "test.as_a_dep.embedded": &test_as_a_dep_embedded/1,
      "test.as_a_dep.standalone": &test_as_a_dep_standalone/1,
      "test.apps": &test_apps/1,
      start_dev: "cmd docker compose up -d",
      stop_dev: "cmd docker compose down -v"
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      before_closing_head_tag: docs_before_closing_head_tag()
    ]
  end

  defp docs_live? do
    System.get_env("MIX_DOCS_LIVE", "false") == "true"
  end

  defp docs_before_closing_head_tag do
    if docs_live?(),
      do: fn
        :html -> ~s[<script type="text/javascript" src="http://livejs.com/live.js"></script>]
        _ -> ""
      end,
      else: fn _ -> "" end
  end

  defp package do
    [
      links: %{
        "Source code" => "https://github.com/electric-sql/phoenix_sync"
      },
      licenses: ["Apache-2.0"],
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp description do
    "Real-time sync for Postgres-backed Phoenix applications."
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_as_a_dep_embedded(args) do
    do_test_as_a_dep("tmp/as_a_dep_embedded", [{:electric, "~> 1.0"}], args)
  end

  defp test_as_a_dep_standalone(args) do
    do_test_as_a_dep("tmp/as_a_dep_standalone", [], args)
  end

  defp do_test_as_a_dep(dir, extra_deps, args) do
    IO.puts("==> Running tests in Phoenix Sync as a dependency")

    dep_names = Enum.map(extra_deps, &elem(&1, 0))
    IO.puts("==> Compiling phoenix_sync from a dependency with: #{inspect(dep_names)}")

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    File.cd!(dir, fn ->
      write_mixfile(extra_deps)

      mix_cmd_with_status_check([
        "do",
        "deps.get,",
        "compile",
        "--force",
        "--warnings-as-errors" | args
      ])
    end)
  end

  defp write_mixfile(extra_deps) do
    deps = [{:phoenix_sync, path: "../.."} | extra_deps]

    File.write!("mix.exs", """
    defmodule DepsOnPhoenixSync.MixProject do
      use Mix.Project

      def project do
        [
          app: :deps_on_phoenix_sync,
          version: "0.0.1",
          deps: #{inspect(deps)}
        ]
      end
    end
    """)
  end

  defp test_apps(args) do
    IO.puts("==> Running tests in Phoenix Sync example apps")

    Path.wildcard("apps/*")
    |> Enum.each(fn app ->
      File.cd!(app, fn ->
        mix_cmd_with_status_check(["deps.get"])

        mix_cmd_with_status_check([
          "test",
          "--force",
          "--warnings-as-errors" | args
        ])
      end)
    end)
  end

  defp mix_cmd_with_status_check(args, opts \\ []) do
    {_, res} = System.cmd("mix", args, [into: IO.binstream(:stdio, :line)] ++ opts)

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
