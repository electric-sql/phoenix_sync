defmodule PhoenixSyncExample.Repo do
  use Phoenix.Sync.Sandbox.Postgres

  use Ecto.Repo,
    otp_app: :phoenix_sync_example,
    adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
end
