defmodule Support.Repo do
  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Ecto.Adapters.Postgres
end


defmodule Support.ConfigTestRepo do
  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Ecto.Adapters.Postgres
end
