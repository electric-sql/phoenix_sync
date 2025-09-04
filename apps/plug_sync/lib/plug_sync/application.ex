defmodule PlugSync.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    server =
      if Application.get_env(:plug_sync, :server, false) do
        [
          {Bandit, plug: {PlugSync.Router, phoenix_sync: Phoenix.Sync.plug_opts()}, port: 4444}
        ]
      else
        []
      end

    children =
      [
        PlugSync.Repo
      ] ++ server

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PlugSync.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
