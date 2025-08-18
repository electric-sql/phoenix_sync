defmodule TxidMatch do
  require Logger

  def query!(sql, params \\ []) do
    Logger.debug("Executing SQL: #{sql} with params: #{inspect(params)}")
    Postgrex.query!(TxidMatch.Postgrex, sql, params)
  end

  def txid do
    # mix txid "txid_current()"
    # mix txid "pg_current_xact_id()::xid"
    Application.fetch_env!(:txid_match, :function)
  end
end
