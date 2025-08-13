defmodule TxidMatch.Migrator do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    TxidMatch.query!("DROP TABLE IF EXISTS txns")

    TxidMatch.query!("CREATE TABLE IF NOT EXISTS txns (id int8 PRIMARY KEY, value varchar(64))")

    :ignore
  end
end
