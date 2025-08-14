defmodule TxidMatch.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Logger.put_application_level(:electric, :warning)
    Logger.put_application_level(:electric_client, :warning)

    children = [
      TxidMatch.Expectations,
      {Postgrex,
       Application.get_env(:txid_match, Postgrex, [])
       |> Keyword.merge(name: TxidMatch.Postgrex, ssl: false, pool_size: 500)},
      TxidMatch.Migrator,
      TxidMatch.Reader,
      {TxidMatch.Writer.Supervisor,
       Application.get_env(:txid_match, TxidMatch.Writer.Supervisor, [])}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TxidMatch.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
