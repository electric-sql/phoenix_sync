defmodule TXIDMatch.MixProject do
  use Mix.Project

  def project do
    [
      app: :txid_match,
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
      mod: {TxidMatch.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:electric, "~> 1.1.1", override: true},
      {:electric_client, "~> 0.7", override: true},
      {:nimble_options, "~> 1.1"},
      {:phoenix_live_view, "~> 1.0", optional: true},
      {:plug, "~> 1.0"},
      {:jason, "~> 1.0"},
      {:ecto, "~> 3.13"},
      {:ecto_sql, "~> 3.10", optional: true},
      {:phoenix_sync, path: "../.."},
      {:postgrex, "~> 0.21"}
    ]
  end
end
