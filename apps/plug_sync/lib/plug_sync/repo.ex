defmodule PlugSync.Repo do
  use Phoenix.Sync.Sandbox.Postgres

  use Ecto.Repo,
    otp_app: :plug_sync,
    adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
end
