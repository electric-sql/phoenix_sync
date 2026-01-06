import Config

config :phoenix_sync, mode: :sandbox, env: config_env()

config :plug_sync, PlugSync.Repo,
  username: "postgres",
  password: "password",
  hostname: "localhost",
  database: "plug_sync_test",
  port: 55555,
  pool: Ecto.Adapters.SQL.Sandbox
