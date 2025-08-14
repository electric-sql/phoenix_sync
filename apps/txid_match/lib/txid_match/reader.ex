defmodule TxidMatch.Reader do
  use Task

  def start_link(opts) do
    Task.start_link(__MODULE__, :run, [opts])
  end

  @txid_max 4_294_967_295

  def run(_opts) do
    stream = Phoenix.Sync.Client.stream(table: "txns")

    Enum.reduce(stream, {0, 0, now()}, fn
      %Electric.Client.Message.ChangeMessage{
        value: %{"id" => id, "value" => value},
        headers: %{
          txids: txids,
          operation: :update
        }
      },
      {count, last_txid, start_time} ->
        Enum.each(txids, fn txid ->
          TxidMatch.Expectations.expect!(id, value, txid)
        end)

        new_last_txid = Enum.max(txids)

        if last_txid && rem(count, 100) == 0 do
          new_last_txid = Enum.max(txids)
          txid_diff = new_last_txid - last_txid
          time = now()
          time_diff = time - start_time

          txid_per_s = txid_diff / (time_diff / 1000.0)
          hrs = (@txid_max - new_last_txid) / txid_per_s / 3600
          days = hrs / 24

          IO.puts(
            "Processed #{fmt(count)} records: #{fmt(new_last_txid)} (#{time_diff}ms; +#{fmt(txid_diff)}; #{fmt(round(txid_per_s))}tx/s) #{Float.round(hrs, 2)}h; #{Float.round(days, 1)}d"
          )

          {count + 1, new_last_txid, time}
        else
          if last_txid,
            do: {count + 1, last_txid, start_time},
            else: {count + 1, new_last_txid, start_time}
        end

      _msg, acc ->
        acc
    end)
  end

  defp now, do: System.monotonic_time(:millisecond)

  def fmt(number) do
    number
    |> Integer.to_string()
    |> to_charlist()
    |> Enum.reverse()
    |> Enum.chunk_every(3)
    |> Enum.map(fn chunk ->
      chunk
      |> Enum.reverse()
      |> to_string()
    end)
    |> Enum.reverse()
    |> Enum.join(",")
  end
end
