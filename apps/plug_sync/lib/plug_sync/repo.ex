defmodule PlugSync.Repo do
  use Ecto.Repo,
    otp_app: :plug_sync,
    adapter: Ecto.Adapters.Postgres
end
