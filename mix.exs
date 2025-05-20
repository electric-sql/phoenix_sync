defmodule Phoenix.Sync.MixProject do
  use Mix.Project

  # Remember to update the README when you change the version
  @version "0.4.3"

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
      aliases: [
        "test.all": ["test", "test.as_a_dep"],
        "test.as_a_dep": &test_as_a_dep/1
      ],
      preferred_cli_env: ["test.all": :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Phoenix.Sync.Application, []}
    ]
  end

  defp deps do
    [
      {:nimble_options, "~> 1.1"},
      {:phoenix_live_view, "~> 1.0", optional: true},
      {:plug, "~> 1.0"},
      {:jason, "~> 1.0"},
      # {:ecto_sql, "~> 3.10", optional: true},
      {:ecto_sql, path: "../ecto_sql", override: true},
      # {:electric, "~> 1.0.6", optional: true},
      {:electric, path: "../electric/packages/sync-service", optional: true, override: true},
      # {:electric_client, ">= 0.5.0-beta-1"}
      {:electric_client, path: "../electric/packages/elixir-client"}
    ] ++ deps_for_env(Mix.env())
  end

  defp deps_for_env(:test) do
    [
      {:floki, "~> 0.36", only: [:test]},
      {:bandit, "~> 1.5", only: [:test], override: true},
      {:uuid, "~> 1.1", only: [:test]},
      {:mox, "~> 1.1", only: [:test]}
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

  defp test_as_a_dep(args) do
    IO.puts("==> Compiling ecto_sql from a dependency")
    File.rm_rf!("tmp/as_a_dep")
    File.mkdir_p!("tmp/as_a_dep")

    File.cd!("tmp/as_a_dep", fn ->
      File.write!("mix.exs", """
      defmodule DepsOnPhoenixSync.MixProject do
        use Mix.Project

        def project do
          [
            app: :deps_on_ecto_sql,
            version: "0.0.1",
            deps: [{:phoenix_sync, path: "../.."}, {:electric, "~> 1.0"}]
          ]
        end
      end
      """)

      mix_cmd_with_status_check(["do", "deps.get,", "compile", "--force" | args])
    end)
  end

  defp mix_cmd_with_status_check(args, opts \\ []) do
    {_, res} = System.cmd("mix", args, [into: IO.binstream(:stdio, :line)] ++ opts)

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
