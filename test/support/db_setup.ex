defmodule Support.DbSetup do
  import ExUnit.Callbacks

  @postgrex_start_opts [
    backoff_type: :stop,
    max_restarts: 0,
    pool_size: 2,
    types: PgInterop.Postgrex.Types
  ]

  def with_unique_db(ctx) do
    base_config = Application.fetch_env!(:electric, :connection_opts)
    {:ok, utility_pool} = start_db_pool(base_config)
    Process.unlink(utility_pool)

    full_db_name = to_string(ctx.test)

    db_name_hash = small_hash(full_db_name)

    # Truncate the database name to 63 characters, use hash to guarantee uniqueness
    db_name = "#{db_name_hash} ~ #{String.slice(full_db_name, 0..50)}"

    escaped_db_name = :binary.replace(db_name, ~s'"', ~s'""', [:global])

    Postgrex.query!(utility_pool, "DROP DATABASE IF EXISTS \"#{escaped_db_name}\"", [])
    Postgrex.query!(utility_pool, "CREATE DATABASE \"#{escaped_db_name}\"", [])

    Enum.each(database_settings(ctx), fn setting ->
      Postgrex.query!(utility_pool, "ALTER DATABASE \"#{db_name}\" SET #{setting}", [])
    end)

    on_exit(fn ->
      Process.link(utility_pool)

      # Make multiple 100ms-spaced attempts to drop the DB because sometimes the replication stream takes some time to stop
      Enum.reduce_while(1..3, :ok, fn _, _ ->
        case Postgrex.query(utility_pool, "DROP DATABASE \"#{escaped_db_name}\"", []) do
          {:ok, _} -> {:halt, :ok}
          {:error, %{postgres: %{code: :object_in_use}}} -> {:cont, Process.sleep(100)}
        end
      end)

      GenServer.stop(utility_pool)
    end)

    updated_config = Keyword.put(base_config, :database, db_name)
    {:ok, pool} = start_db_pool(updated_config)

    {:ok, %{utility_pool: utility_pool, db_config: updated_config, pool: pool, db_conn: pool}}
  end

  def with_publication(ctx) do
    publication_name = "electric_test_publication_#{small_hash(ctx.test)}"
    Postgrex.query!(ctx.pool, "CREATE PUBLICATION \"#{publication_name}\"", [])
    {:ok, %{publication_name: publication_name}}
  end

  def with_pg_version(ctx) do
    %{rows: [[pg_version]]} =
      Postgrex.query!(ctx.db_conn, "SELECT current_setting('server_version_num')::integer", [])

    {:ok, %{pg_version: pg_version}}
  end

  def with_shared_db(_ctx) do
    config = Application.fetch_env!(:electric, :connection_opts)
    {:ok, pool} = start_db_pool(config)
    {:ok, %{pool: pool, db_config: config, db_conn: pool}}
  end

  def in_transaction(%{pool: pool}) do
    parent = self()

    {:ok, task} =
      Task.start(fn ->
        Postgrex.transaction(
          pool,
          fn conn ->
            send(parent, {:conn_handover, conn})

            exit_parent =
              receive do
                {:done, exit_parent} -> exit_parent
              end

            Postgrex.rollback(conn, {:complete, exit_parent})
          end,
          timeout: :infinity
        )
        |> case do
          {:error, {:complete, target}} ->
            send(target, :transaction_complete)

          {:error, _} ->
            receive do
              {:done, target} -> send(target, :transaction_complete)
            end
        end
      end)

    conn =
      receive do
        {:conn_handover, conn} -> conn
      end

    on_exit(fn ->
      send(task, {:done, self()})

      receive do
        :transaction_complete -> :ok
      after
        5000 -> :ok
      end
    end)

    {:ok, %{db_conn: conn}}
  end

  defp database_settings(%{database_settings: settings}), do: settings
  defp database_settings(_), do: []

  defp small_hash(value),
    do:
      to_string(value)
      |> :erlang.phash2(64 ** 5)
      |> :binary.encode_unsigned()
      |> Base.encode64()
      |> String.replace_trailing("==", "")

  defp start_db_pool(connection_opts) do
    start_opts = Electric.Utils.deobfuscate_password(connection_opts) ++ @postgrex_start_opts
    Postgrex.start_link(start_opts)
  end

  def with_table(ctx) do
    case ctx do
      %{table: {name, columns}} ->
        schema = schema_name(name)

        Postgrex.query!(
          ctx.db_conn,
          "CREATE SCHEMA IF NOT EXISTS #{inspect_relation(schema)}",
          []
        )

        on_exit(fn ->
          if schema != "public" do
            Postgrex.query!(
              ctx.utility_pool,
              "DROP SCHEMA IF EXISTS #{inspect_relation(schema)}",
              []
            )
          end
        end)

        sql = """
        CREATE TABLE #{inspect_relation(name)} (
        #{Enum.join(columns, ",\n")}
        )
        """

        Postgrex.query!(ctx.db_conn, sql, [])
        :ok

      _ ->
        :ok
    end
  end

  def with_data(ctx) do
    case Map.get(ctx, :data, nil) do
      {table, columns, values} ->
        placeholders =
          columns |> Enum.with_index(1) |> Enum.map_join(", ", fn {_, n} -> "$#{n}" end)

        query =
          ~s|INSERT INTO #{inspect_relation(table)} (#{Enum.map_join(columns, ", ", &~s|"#{&1}"|)}) VALUES (#{placeholders})|

        for params <- values, do: Postgrex.query!(ctx.db_conn, query, params)

        :ok

      nil ->
        :ok
    end
  end

  def inspect_relation(name) when is_binary(name) do
    inspect(name)
  end

  def inspect_relation({schema, name}) do
    "#{inspect(schema)}.#{inspect(name)}"
  end

  def schema_name(name) when is_binary(name) do
    "public"
  end

  def schema_name({schema, _name}) do
    schema
  end
end
