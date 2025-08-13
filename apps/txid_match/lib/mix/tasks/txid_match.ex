defmodule Mix.Tasks.Txid do
  use Mix.Task

  @shortdoc "Run the TXID Match Task"

  def run(args) do
    {_, [function], _} = OptionParser.parse(args, strict: [])

    Mix.shell().info("Running TXID Match Task with function: #{function}")

    Application.put_env(:txid_match, :function, function, persistent: true)

    db_config = Application.get_env(:txid_match, Postgrex, [])

    {:ok, database} = Keyword.fetch(db_config, :database)
    {:ok, host} = Keyword.fetch(db_config, :hostname)
    {:ok, port} = Keyword.fetch(db_config, :port)
    {:ok, username} = Keyword.fetch(db_config, :username)
    {:ok, password} = Keyword.fetch(db_config, :password)

    System.put_env("PGPASSWORD", password)

    {out, 0} =
      System.cmd("psql", [
        "--host",
        host,
        "--port",
        "#{port}",
        "--username",
        username,
        "--tuples-only",
        "--csv",
        "--list"
      ])

    dbs =
      out
      |> String.split("\n")
      |> Enum.map(&String.split(&1, ","))
      |> Enum.map(&Enum.at(&1, 0))

    if database in dbs do
      {_, 0} =
        System.cmd("dropdb", [
          "--host",
          host,
          "--port",
          "#{port}",
          "--username",
          username,
          "--force",
          database
        ])

      Mix.shell().info("Dropped database #{database}")
    end

    {_, 0} =
      System.cmd("createdb", [
        "-T",
        "template0",
        "-E",
        "UTF-8",
        "--host",
        host,
        "--port",
        "#{port}",
        "--username",
        username,
        database
      ])

    Mix.shell().info("Created database #{database}")

    Mix.Task.run("app.start", args)

    # # Ensure the application is started
    # Application.ensure_all_started(:txid_match)

    # Run the main logic of the task
    IO.puts("Running TXID Match Task with arguments: #{inspect(args)}")

    # Here you can add the logic for your task
    # For example, you might want to call a function from your application module
    # TXIDMatch.SomeModule.some_function(args)
    Process.sleep(:infinity)
  end
end
