import Config


connection_opts = [
  username: "garry",
  password: "password",
  hostname: "localhost",
  database: "txid_sync",
  port: 5432
]

config :txid_match, Postgrex, connection_opts

config :phoenix_sync,
  env: config_env(),
  mode: :embedded,
  connection_opts: [{:sslmode, :disable} | connection_opts]
