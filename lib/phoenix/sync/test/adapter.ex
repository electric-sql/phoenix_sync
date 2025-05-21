defmodule Phoenix.Sync.Test.Adapter do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Transaction

  @adapter Ecto.Adapters.Postgres
  @driver :postgrex

  @impl true
  defmacro __before_compile__(env) do
    Ecto.Adapters.SQL.__before_compile__(@driver, env)
  end

  @impl true
  def ensure_all_started(config, type) do
    @adapter.ensure_all_started(config, type)
  end

  @impl true
  def init(config) do
    with {:ok, spec, meta} <- @adapter.init(config) |> dbg do
      # {:ok, spec, Map.merge(meta, %{sync: self(), sql: __MODULE__.Connection})}
      {:ok, spec, Map.merge(meta, %{sync: self()})}
    end
  end

  @impl true
  def checkout(meta, opts, fun) do
    dbg(meta)
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
    # dbg(adapter_insert: {adapter_meta, schema_meta, params, returning})
    pks = schema_meta.schema.__schema__(:primary_key)
    all_columns = schema_meta.schema.__schema__(:fields)

    {:ok, inserted} =
      @adapter.insert(adapter_meta, schema_meta, params, on_conflict, all_columns, opts)

    dbg(inserted)

    {:ok, Keyword.take(inserted, returning)}
  end

  @impl true
  def update(adapter_meta, schema_meta, fields, params, returning, opts) do
    # dbg(adapter_update: {adapter_meta, schema_meta, fields, params})
    all_columns = schema_meta.schema.__schema__(:fields)
    _previous = adapter_meta.repo.get_by(schema_meta.schema, params) |> dbg
    {:ok, updated} = @adapter.update(adapter_meta, schema_meta, fields, params, all_columns, opts)
    dbg(updated)
    {:ok, Keyword.take(updated, returning)}
  end

  @impl true
  def delete(adapter_meta, schema_meta, params, returning, opts) do
    dbg(adapter_delete: {adapter_meta, schema_meta, params})
    all_columns = schema_meta.schema.__schema__(:fields)
    {:ok, deleted} = @adapter.delete(adapter_meta, schema_meta, params, all_columns, opts)
    dbg(deleted)
    {:ok, Keyword.take(deleted, returning)}
  end

  ## Transaction

  @impl true
  def transaction(meta, opts, fun) do
    dbg(transaction: meta)
    @adapter.transaction(meta, opts, fun) |> dbg
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
      dbg(conn_insert: {prefix, table, header, rows, on_conflict, returning, placeholders})
      @connection.insert(prefix, table, header, rows, on_conflict, returning, placeholders)
    end

    @impl true
    def update(prefix, table, fields, filters, returning) do
      dbg(conn_update: {prefix, table, fields, filters})
      @connection.update(prefix, table, fields, filters, returning)
    end

    @impl true
    def delete(prefix, table, filters, returning) do
      dbg(conn_delete: {prefix, table, filters, returning})
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
