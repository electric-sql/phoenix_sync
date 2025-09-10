defmodule PlugSync.MixProject do
  use Mix.Project

  def project do
    [
      app: :plug_sync,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {PlugSync.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:plug, "~> 1.0"},
      {:bandit, "~> 1.0"},
      {:postgrex, "~> 0.21"},
      {:ecto_sql, "~> 3.0"},
      {:electric, "~> 1.1.2"},
      {:phoenix_sync, [path: "../..", override: true]},
      {:igniter, "~> 0.6"}
    ]
  end
end
