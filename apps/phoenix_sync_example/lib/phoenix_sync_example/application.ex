defmodule PhoenixSyncExample.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      PhoenixSyncExampleWeb.Telemetry,
      PhoenixSyncExample.Repo,
      {DNSCluster,
       query: Application.get_env(:phoenix_sync_example, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: PhoenixSyncExample.PubSub},
      # Start a worker by calling: PhoenixSyncExample.Worker.start_link(arg)
      # {PhoenixSyncExample.Worker, arg},
      # Start to serve requests, typically the last entry
      PhoenixSyncExampleWeb.Endpoint
      # PhoenixSyncExample.Sync
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PhoenixSyncExample.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    PhoenixSyncExampleWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
