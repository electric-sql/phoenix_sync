defmodule TxidMatch.Writer.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(args) do
    writer_count = Keyword.get(args, :writer_count, 1)
    bumper_count = Keyword.get(args, :bumper_count, div(System.schedulers_online(), 1))

    IO.inspect(["writers: #{writer_count}; bumpers: #{bumper_count}"])

    children =
      Enum.concat([
        Enum.map(1..writer_count, fn n ->
          {TxidMatch.Writer, n}
        end),
        Enum.map(1..bumper_count, fn n ->
          {TxidMatch.Bumper, n}
        end)
      ])

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    Supervisor.init(children, strategy: :one_for_one)
  end
end
