defmodule Phoenix.Sync.Test.Adapter do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Transaction

  @adapter Ecto.Adapters.Postgres
  @driver :postgrex

  require DBConnection.Holder

  @impl true
  defmacro __before_compile__(env) do
    Ecto.Adapters.SQL.__before_compile__(@driver, env)
  end

  @impl true
  def ensure_all_started(config, type) do
    dbg(ensure_all_started: config)
    @adapter.ensure_all_started(config, type)
  end

  @impl true
  def init(config) do
    with {:ok, spec, meta} <- @adapter.init(config) do
      dbg(meta)
      {:ok, spec, Map.merge(meta, %{sync: self()})}
    end
  end

  @impl true
  def checkout(meta, opts, fun) do
    @adapter.checkout(meta, opts, fun)
  end

  @impl true
  def checked_out?(meta) do
    @adapter.checked_out?(meta)
  end

  @impl true
  def loaders(primitive_type, ecto_type), do: @adapter.loaders(primitive_type, ecto_type)

  @impl true
  def dumpers(primitive_type, ecto_type), do: @adapter.dumpers(primitive_type, ecto_type)

  ## Query

  @impl true
  def prepare(type, query) do
    @adapter.prepare(type, query)
  end

  @impl true
  def execute(adapter_meta, query_meta, query, params, opts) do
    # dbg(execute: adapter_meta)
    @adapter.execute(adapter_meta, query_meta, query, params, opts)
  end

  @impl true
  def stream(adapter_meta, query_meta, query, params, opts) do
    @adapter.stream(adapter_meta, query_meta, query, params, opts)
  end

  ## Schema

  @impl true
  def autogenerate(type), do: @adapter.autogenerate(type)

  @impl true
  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        on_conflict,
        returning,
        placeholders,
        opts
      ) do
    @adapter.insert_all(
      adapter_meta,
      schema_meta,
      header,
      rows,
      on_conflict,
      returning,
      placeholders,
      opts
    )
  end

  @impl true
  def insert(adapter_meta, schema_meta, params, on_conflict, returning, opts) do
    pks = schema_meta.schema.__schema__(:primary_key)
    all_columns = schema_meta.schema.__schema__(:fields)

    {:ok, inserted} =
      @adapter.insert(adapter_meta, schema_meta, params, on_conflict, all_columns, opts)

    Phoenix.Sync.Test.Sandbox.Producer.emit_changes(conn!(adapter_meta), [
      {:insert, schema_meta, inserted}
    ])

    {:ok, Keyword.take(inserted, returning)}
  end

  defp quote_names(names) do
    Enum.map_intersperse(names, ?,, &quote_name/1)
  end

  defp quote_name(nil, name), do: quote_name(name)

  defp quote_name(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

  defp quote_name({prefix, name}) do
    quote_name(prefix, name)
  end

  defp quote_name(name) when is_atom(name) do
    quote_name(Atom.to_string(name))
  end

  defp quote_name(name) when is_binary(name) do
    if String.contains?(name, "\"") do
      raise ArgumentError,
            "bad literal/field/index/table name #{inspect(name)} (\" is not permitted)"
    end

    [?", name, ?"]
  end

  defp intersperse_reduce(list, separator, user_acc, reducer, acc \\ [])

  defp intersperse_reduce([], _separator, user_acc, _reducer, acc),
    do: {acc, user_acc}

  defp intersperse_reduce([elem], _separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    {[acc | elem], user_acc}
  end

  defp intersperse_reduce([elem | rest], separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    intersperse_reduce(rest, separator, user_acc, reducer, [acc, elem, separator])
  end

  defp returning([]),
    do: []

  defp returning(returning),
    do: [" RETURNING " | quote_names(returning)]

  def conn(%{pid: pool} = _adapter_meta) do
    conn(pool)
  end

  def conn(pool) do
    # inner_pool = GenServer.call(pool, :conn) |> dbg
    # dbg(inner_pool)

    case Process.get({Ecto.Adapters.SQL, pool}) do
      %DBConnection{} = conn ->
        conn

      nil ->
        case DBConnection.Ownership.ownership_checkout(pool, []) do
          {:already, state} when state in [:allowed, :owner] ->
            DBConnection.run(pool, fn conn -> conn end)

          :ok ->
            DBConnection.run(pool, fn conn -> conn end)
        end
    end
    |> case do
      %DBConnection{pool_ref: pool_ref} ->
        DBConnection.Holder.pool_ref(pool: conn) = pool_ref
        {:ok, conn}

      invalid ->
        {:error, {:noconnection, invalid}}
    end
  end

  def conn!(pool) do
    {:ok, conn} = conn(pool)
    conn
  end

  @impl true
  def update(adapter_meta, schema_meta, fields, params, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta

    # This is adapted from this function:
    # sql = Ecto.Adapters.Postgres.Connection.update(prefix, source, fields, params, [])

    {fields, field_values} = :lists.unzip(fields)
    filter_values = Keyword.values(params)

    {fields, count} =
      intersperse_reduce(fields, ", ", 1, fn field, acc ->
        {[quote_name(field), " = $" | Integer.to_string(acc)], acc + 1}
      end)

    {filters_new, _count} = update_filters(params, count, &quote_name("new", &1))
    {filters_old, _count} = update_filters(params, count, &quote_name/1)

    all_columns = schema_meta.schema.__schema__(:fields)
    cols = Enum.map_intersperse(all_columns, ?,, &quote_name/1)
    return_all = Enum.map(all_columns, &{:old, &1}) ++ Enum.map(all_columns, &{:new, &1})

    # https://stackoverflow.com/a/7927957
    # return the old and new values from an update by aquiring an exclusive lock
    # on the row and using a sub-query.
    # UPDATE table new
    #     SET value = $1
    #     FROM (select id, value FROM table WHERE id = $2 FOR UPDATE) old
    #     WHERE new.id = $2
    #     RETURNING old.id, old.value, new.id, new.value;
    #
    # TODO: should be re-written as which prevents problems when updating the pk
    # UPDATE table new
    #     SET value = $1
    #     FROM (select id, value FROM table WHERE id = $2 FOR UPDATE) old
    #     -- HERE
    #     WHERE new.id = old.id
    #     RETURNING old.id, old.value, new.id, new.value;

    sql = [
      "UPDATE ",
      quote_name(prefix, source),
      " new SET ",
      fields,
      " FROM (SELECT ",
      cols,
      " FROM ",
      quote_name(prefix, source),
      " WHERE ",
      filters_old,
      " FOR UPDATE) old",
      " WHERE ",
      filters_new | returning(return_all)
    ]

    with {:ok, updated} <-
           Ecto.Adapters.SQL.struct(
             adapter_meta,
             Ecto.Adapters.Postgres.Connection,
             sql,
             :update,
             source,
             params,
             field_values ++ filter_values,
             :raise,
             return_all,
             opts
           ) do
      {new, old} =
        Enum.reduce(updated, {[], []}, fn
          {{:new, k}, v}, {new, old} -> {[{k, v} | new], old}
          {{:old, k}, v}, {new, old} -> {new, [{k, v} | old]}
        end)

      Phoenix.Sync.Test.Sandbox.Producer.emit_changes(conn!(adapter_meta), [
        {:update, schema_meta, old, new}
      ])

      {:ok, Keyword.take(new, returning)}
    end
  end

  defp update_filters(params, count, quote_fun) do
    intersperse_reduce(params, " AND ", count, fn
      {field, nil}, acc ->
        {[quote_fun.(field), " IS NULL"], acc}

      {field, _value}, acc ->
        {[quote_fun.(field), " = $" | Integer.to_string(acc)], acc + 1}
    end)
  end

  @impl true
  def delete(adapter_meta, schema_meta, params, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    all_columns = schema_meta.schema.__schema__(:fields)
    {:ok, deleted} = @adapter.delete(adapter_meta, schema_meta, params, all_columns, opts)

    Phoenix.Sync.Test.Sandbox.Producer.emit_changes(conn!(adapter_meta), [
      {:delete, schema_meta, deleted}
    ])

    {:ok, Keyword.take(deleted, returning)}
  end

  ## Transaction

  @impl true
  def transaction(meta, opts, fun) do
    @adapter.transaction(meta, opts, fun)
  end

  @impl true
  def in_transaction?(meta) do
    @adapter.in_transaction?(meta)
  end

  @impl true
  def rollback(meta, value) do
    @adapter.rollback(meta, value)
  end

  ## Migration

  @impl true
  def execute_ddl(meta, definition, opts) do
    @adapter.execute_ddl(meta, definition, opts)
  end

  @impl true
  def supports_ddl_transaction? do
    @adapter.supports_ddl_transaction?()
  end

  @impl true
  def lock_for_migrations(meta, opts, fun) do
    @adapter.lock_for_migrations(meta, opts, fun)
  end

  defmodule Connection do
    @behaviour Ecto.Adapters.SQL.Connection

    @connection Ecto.Adapters.Postgres.Connection

    def child_spec(opts) do
      @connection.child_spec(opts)
    end

    @impl true
    def to_constraints(exception, options),
      do: @connection.to_constraints(exception, options)

    @impl true
    def prepare_execute(conn, name, sql, params, opts) do
      @connection.prepare_execute(conn, name, sql, params, opts)
    end

    @impl true
    def query(conn, sql, params, opts) do
      # TODO: parse the query to detect inserts?
      # dbg(query: sql)
      @connection.query(conn, sql, params, opts)
    end

    @impl true
    def query_many(conn, sql, params, opts) do
      @connection.query_many(conn, sql, params, opts)
    end

    @impl true
    def execute(conn, query, params, opts) do
      # dbg(execute: query)
      @connection.execute(conn, query, params, opts)
    end

    @impl true
    def stream(conn, sql, params, opts) do
      @connection.stream(conn, sql, params, opts)
    end

    @impl true
    def all(query, as_prefix \\ []) do
      @connection.all(query, as_prefix)
    end

    def update_all(query, prefix \\ nil) do
      @connection.update_all(query, prefix)
    end

    @impl true
    def delete_all(query) do
      @connection.delete_all(query)
    end

    @impl true
    def insert(prefix, table, header, rows, on_conflict, returning, placeholders) do
      # dbg(conn_insert: {prefix, table, header, rows, on_conflict, returning, placeholders})
      @connection.insert(prefix, table, header, rows, on_conflict, returning, placeholders)
    end

    @impl true
    def update(prefix, table, fields, filters, returning) do
      # dbg(conn_update: {prefix, table, fields, filters})
      @connection.update(prefix, table, fields, filters, returning)
    end

    @impl true
    def delete(prefix, table, filters, returning) do
      # dbg(conn_delete: {prefix, table, filters, returning})
      @connection.delete(prefix, table, filters, returning)
    end

    @impl true
    def explain_query(conn, query, params, opts) do
      @connection.explain_query(conn, query, params, opts)
    end

    @impl true
    def execute_ddl(command) do
      @connection.execute_ddl(command)
    end

    @impl true
    def ddl_logs(result) do
      @connection.ddl_logs(result)
    end

    @impl true
    def table_exists_query(table) do
      @connection.table_exists_query(table)
    end
  end
end
