defmodule TxidMatch.Writer do
  def child_spec(id) when is_integer(id) do
    %{
      id: {__MODULE__, id},
      start: {Task, :start_link, [__MODULE__, :run, [id]]},
      type: :worker,
      restart: :permanent
    }
  end

  def run(id) do
    TxidMatch.query!(
      "INSERT INTO txns (id, value) VALUES ($1, $2)",
      [id, value()]
    )

    DBConnection.run(
      TxidMatch.Postgrex,
      fn conn ->
        query =
          Postgrex.prepare!(
            conn,
            "update",
            "UPDATE txns SET value = $1 WHERE id = $2 RETURNING #{TxidMatch.txid()}"
          )

        loop(conn, query, id)
      end,
      timeout: :infinity
    )
  end

  defp loop(conn, query, id) do
    value = value()

    %{rows: [[txid]]} = DBConnection.execute!(conn, query, [value, id])

    TxidMatch.Expectations.set(id, value, txid)

    Process.sleep(100)
    loop(conn, query, id)
  end

  def value do
    <<System.monotonic_time()::little-64, :crypto.strong_rand_bytes(12)::binary>>
    |> Base.encode32(case: :lower)
  end
end
