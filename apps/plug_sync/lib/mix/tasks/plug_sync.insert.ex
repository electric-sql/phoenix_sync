defmodule Mix.Tasks.PlugSync.Insert do
  use Mix.Task

  @shortdoc "Inserts sample data into the database for testing PlugSync"

    alias PlugSync.Repo
    alias PlugSync.Tasks.Task
    alias PlugSync.Tasks.Step

  def run(args) do
    {opts, _, _} = OptionParser.parse(args, strict: [rows: :integer, data_size: :integer])

    # {:ok, repo} = Repo.start_link()
    Application.ensure_all_started(:plug_sync) |> dbg

    data_size = Keyword.get(opts, :data_size, 1024)
    rows = Keyword.get(opts, :rows, 1024)

    Repo.transaction(fn ->

      items = Stream.repeatedly(fn -> item(data_size) end) |> Enum.take(rows)
      Repo.insert_all(PlugSync.Item, items)

    end)

    IO.puts("Sample data inserted successfully.")
  end

  defp item(data_size) do
    %{
      value: Enum.random(1..100_000),
      data: :crypto.strong_rand_bytes(div(data_size , 2) ) |> Base.encode16(),
      inserted_at: NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second),
      updated_at: NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second),
    }
  end
end
