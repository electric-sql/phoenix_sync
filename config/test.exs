import Config

config :logger,
  level: :warning,
  compile_time_purge_matching: [
    [application: :electric, level_lower_than: :error]
  ]

config :phoenix_sync, Phoenix.Sync.LiveViewTest.Endpoint, []

db_config = [
  username: "postgres",
  password: "password",
  hostname: "localhost",
  database: "phoenix_sync",
  port: 55555
]

# configure the support repo with random options so we can validate them in Phoenix.Sync.ConfigTest
config :phoenix_sync,
       Support.Repo,
       db_config ++
         [
           stacktrace: true,
           show_sensitive_data_on_connection_error: true,
           pool_size: 10,
           pool: Ecto.Adapters.SQL.Sandbox
         ]

config :phoenix_sync,
       Support.ConfigTestRepo,
       Keyword.delete(db_config, :port) ++
         [
           stacktrace: true,
           show_sensitive_data_on_connection_error: true,
           pool_size: 10
         ]

config :phoenix_sync,
       Support.SandboxRepo,
       db_config ++
         [
           stacktrace: true,
           show_sensitive_data_on_connection_error: true,
           pool: Ecto.Adapters.SQL.Sandbox,
           pool_size: 10,
           ownership_log: :warning
         ]

config :phoenix_sync, env: :test, mode: :sandbox

config :phoenix_sync,
       Phoenix.Sync.SandboxTest.Endpoint,
       secret_key_base: "GEp12Mvwia8mEwCJ",
       live_view: [signing_salt: "GEp12Mvwia8mEwCJ"]
