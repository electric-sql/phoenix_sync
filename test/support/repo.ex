defmodule Support.Repo do
  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Ecto.Adapters.Postgres
end

defmodule Support.SandboxRepo do
  use Phoenix.Sync.Sandbox.Postgres

  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
end

defmodule Support.ConfigTestRepo do
  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Ecto.Adapters.Postgres
end
