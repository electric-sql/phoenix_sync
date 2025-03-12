import Config

config :logger, level: :critical

config :phoenix_sync, mode: :disabled

config :phoenix_sync, Phoenix.Sync.LiveViewTest.Endpoint, []

config :phoenix_sync, Electric.Client, base_url: "http://localhost:3000"

# configure the support repo with random options so we can validate them in Phoenix.Sync.ConfigTest
config :phoenix_sync, Support.Repo,
  username: "postgres",
  password: "password",
  hostname: "localhost",
  database: "electric",
  port: 54321,
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :phoenix_sync, Support.ConfigTestRepo,
  username: "postgres",
  password: "password",
  hostname: "localhost",
  database: "electric",
  # phoenix_sync should fill in default port
  # port: 54321,
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  ssl: true,
  socket_options: [:inet6],
  pool_size: 10
