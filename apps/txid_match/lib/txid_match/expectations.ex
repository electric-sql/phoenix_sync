defmodule TxidMatch.Expectations do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def set(id, value, txid) do
    :ets.insert(__MODULE__, {{id, value}, txid})
  end

  def expect!(id, value, txid) do
    case :ets.lookup(__MODULE__, {id, value}) do
      [{_, expected_txid}] when expected_txid == txid ->
        :ets.delete(__MODULE__, {id, value})

      [{_, invalid_txid}] ->
        IO.puts(
          :stderr,
          IO.ANSI.format([
            :red,
            :bright,
            "Expected txid for id #{inspect(id)} and value #{inspect(value)} to be #{inspect(txid)}, but got #{invalid_txid}.\n"
          ])
        )

        System.stop(111)
    end
  end

  def init(_) do
    table =
      :ets.new(
        __MODULE__,
        [
          :named_table,
          :public,
          read_concurrency: true,
          write_concurrency: true,
          keypos: 1
        ]
      )

    {:ok, %{table: table}}
  end
end
