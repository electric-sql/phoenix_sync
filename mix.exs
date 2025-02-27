defmodule Phoenix.Sync.MixProject do
  use Mix.Project

  def project do
    [
      app: :phoenix_sync,
      version: "0.2.0",
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
      homepage_url: "https://electric-sql.com"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Phoenix.Sync.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:nimble_options, "~> 1.1"},
      {:phoenix_live_view, "~> 1.0", optional: true},
      {:plug, "~> 1.0"},
      {:jason, "~> 1.0"},
      {:ecto_sql, "~> 3.10", optional: true},
      {
        :electric,
        optional: true,
        github: "electric-sql/electric",
        sparse: "packages/sync-service",
        ref: "955449869fbc2073b870702359d090080e891670"
      },
      {
        :electric_client,
        github: "electric-sql/electric",
        sparse: "packages/elixir-client",
        ref: "955449869fbc2073b870702359d090080e891670"
      }
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
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp deps_for_env(_) do
    []
  end

  # defp very_temporary_path_based_deps_remove_me! do
  #   [
  #     {:electric,
  #      path: "../electric/packages/sync-service/", only: [:dev, :test], override: true},
  #     {:electric_client,
  #      path: "../electric/packages/elixir-client/", env: :dev, only: [:dev, :test]}
  #   ]
  # end

  defp docs do
    [
      main: "Phoenix.Sync"
    ]
  end

  defp package do
    [
      links: %{
        "Electric SQL" => "https://electric-sql.com"
      },
      licenses: ["Apache-2.0"]
    ]
  end

  defp description do
    "A work-in-progress adapter to integrate Electric SQL's streaming updates into Phoenix."
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
