import Config

if config_env() == :test do
  # port = 3333
  default_database_url = "postgresql://postgres:password@localhost:54321/electric?sslmode=disable"
  database_url = System.get_env("DATABASE_URL", default_database_url)

  config :electric,
    start_in_library_mode: true,
    connection_opts: Electric.Config.parse_postgresql_uri!(database_url),
    # enable the http api so that the client tests against a real endpoint can
    # run against our embedded electric instance.
    # enable_http_api: true,
    # service_port: port,
    allow_shape_deletion?: false,
    # use a non-default replication stream id so we can run the client
    # tests at the same time as an active electric instance
    replication_stream_id: "phoenix_sync_tests",
    storage_dir: Path.join(System.tmp_dir!(), "electric/client-tests#{System.monotonic_time()}")
end
