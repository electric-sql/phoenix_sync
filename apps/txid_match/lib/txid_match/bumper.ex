defmodule TxidMatch.Bumper do
  def child_spec(id) when is_integer(id) do
    %{
      id: {__MODULE__, id},
      start: {Task, :start_link, [__MODULE__, :run, [id]]},
      type: :worker,
      restart: :permanent
    }
  end

  def run(id) do
    DBConnection.run(
      TxidMatch.Postgrex,
      fn conn ->
        query =
          Postgrex.prepare!(
            conn,
            "bump",
            "SELECT pg_current_xact_id()"
          )

        loop(conn, query, id)
      end,
      timeout: :infinity
    )
  end

  defp loop(conn, query, id) do
    %{rows: [[_txid]]} = DBConnection.execute!(conn, query, [])

    loop(conn, query, id)
  end
end
