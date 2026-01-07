defmodule PhoenixSyncExample.Repo do
  use Ecto.Repo,
    otp_app: :phoenix_sync_example,
    adapter: Ecto.Adapters.Postgres
end
