defmodule Mix.Tasks.Txid.Postgres do
  use Mix.Task

  @shortdoc "Manage the in-memory PostgreSQL database for txid_match"

  def run(args) do
    {_, [action], _} = OptionParser.parse(args, strict: [])

    case action do
      "up" -> postgres_up()
      "down" -> postgres_down()
      _ -> Mix.raise("Unknown action: #{action}. Use 'up' or 'down'.")
    end
  end

  defp postgres_up do
    Mix.shell().info("Starting PostgreSQL in-memory database...")
    db_config = Application.get_env(:txid_match, TxidMatch.Postgrex, [])

    {:ok, database} = Keyword.fetch(db_config, :database)
    {:ok, host} = Keyword.fetch(db_config, :hostname)
    {:ok, port} = Keyword.fetch(db_config, :port)
    {:ok, username} = Keyword.fetch(db_config, :username)
    {:ok, password} = Keyword.fetch(db_config, :password)

    System.put_env("PGPASSWORD", password)
    data_dir = data_dir()
    # 2 GB
    disk_size = 2 * 1024 * 1024 * 1024

    File.mkdir_p!(data_dir)
    user = System.get_env("USER") || raise "$USER not present"

    {_, 0} = System.cmd("sudo", ~w(modprobe brd rd_nr=1 rd_size=#{disk_size}))
    {_, 0} = System.cmd("sudo", ~w(mkfs.xfs /dev/ram0))
    {_, 0} = System.cmd("sudo", ~w(mount /dev/ram0 #{data_dir}))
    {_, 0} = System.cmd("sudo", ~w(chown #{user} #{data_dir}))

    File.write!("#{data_dir}/postgresql.conf", """
    listen_addresses = '*'
    wal_level = logical
    max_replication_slots = 100
    max_connections = 1000
    fsync = off
    """)

    {_, 0} = System.cmd("pg_ctrl", ~w[init -D #{data_dir}])
    {_, 0} = System.cmd("pg_ctrl", ~w[start -D #{data_dir}])
  end

  defp postgres_down do
    Mix.shell().info("Stopping PostgreSQL in-memory database...")
    data_dir = data_dir() |> dbg

    {_, _} = System.cmd("pg_ctl", ~w[stop -D #{data_dir}])
    {_, 0} = System.cmd("sudo", ~w(umount #{data_dir}))
    {_, 0} = System.cmd("sudo", ~w(rmmod brd))

    File.rm_rf!(data_dir)
  end

  defp data_dir do
    System.get_env("PGDATA", Path.expand("tmp/pgdata", File.cwd!()))
  end
end
