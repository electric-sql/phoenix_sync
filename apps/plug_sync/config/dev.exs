import Config

config :plug_sync, ecto_repos: [PlugSync.Repo], start_server: false

case System.get_env("PHOENIX_SYNC_MODE", "embedded") do
  "http" ->
    IO.puts("Starting in HTTP mode")

    config :phoenix_sync,
      mode: :http,
      env: config_env(),
      url: "http://localhost:3000"

    config :plug_sync, PlugSync.Repo,
      username: "postgres",
      password: "password",
      hostname: "localhost",
      database: "electric",
      port: 54321

  _ ->
    IO.puts("Starting in embedded mode")

    config :phoenix_sync,
      mode: :embedded,
      env: config_env(),
      repo: PlugSync.Repo

    config :plug_sync, PlugSync.Repo,
      username: "postgres",
      password: "password",
      hostname: "localhost",
      database: "plug_sync",
      port: 55555
end
