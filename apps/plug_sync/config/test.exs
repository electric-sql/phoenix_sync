import Config

config :phoenix_sync, mode: :embedded, env: config_env(), repo: PlugSync.Repo

config :plug_sync, PlugSync.Repo,
  username: "postgres",
  password: "password",
  hostname: "localhost",
  database: "phoenix_sync",
  port: 54321,
  pool: Ecto.Adapters.SQL.Sandbox
