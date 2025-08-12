if Code.ensure_loaded?(Ecto.Adapters.Postgres) do
  defmodule Phoenix.Sync.Sandbox.Postgres.Adapter do
    @moduledoc false

    @behaviour Ecto.Adapter
    @behaviour Ecto.Adapter.Migration
    @behaviour Ecto.Adapter.Queryable
    @behaviour Ecto.Adapter.Schema
    @behaviour Ecto.Adapter.Transaction
    @behaviour Ecto.Adapter.Storage

    @adapter Ecto.Adapters.Postgres
    @driver :postgrex

    @impl true
    defmacro __before_compile__(env) do
      Ecto.Adapters.SQL.__before_compile__(@driver, env)
    end

    @impl true
    defdelegate ensure_all_started(config, type), to: @adapter

    @impl true
    defdelegate init(config), to: @adapter

    @impl true
    defdelegate checkout(meta, opts, fun), to: @adapter

    @impl true
    defdelegate checked_out?(meta), to: @adapter

    @impl true
    defdelegate loaders(primitive_type, ecto_type), to: @adapter

    @impl true
    defdelegate dumpers(primitive_type, ecto_type), to: @adapter

    ## Query

    # prepare(atom :: :all | :update_all | :delete_all, query :: Ecto.Query.t())
    @impl true
    def prepare(:update_all, query) do
      if stack_id = Phoenix.Sync.Sandbox.stack_id() do
        {table, schema, source_prefix} = elem(query.sources, 0)

        [cte, prefix, fields, join, where | returning] =
          Ecto.Adapters.Postgres.Connection.update_all(query)

        [_, source_alias] =
          Regex.run(~r/"#{table}" AS ([a-z0-9]+)/, IO.iodata_to_binary(prefix))

        field_names = schema.__schema__(:fields)

        column_names =
          Enum.map(field_names, &quote_name(to_string(schema.__schema__(:field_source, &1))))

        old_join = [
          "(SELECT ",
          Enum.intersperse(column_names, ", "),
          " FROM ",
          quote_name(source_prefix, table),
          " FOR UPDATE) AS old"
        ]

        join =
          case join do
            [] -> [" FROM " | old_join]
            join -> [join | [", " | old_join]]
          end

        on =
          Enum.map_intersperse(schema.__schema__(:primary_key), " AND ", fn pk ->
            ["old.", quote_name(pk), " = ", source_alias, ".", quote_name(pk)]
          end)

        where =
          case where do
            [] -> [" WHERE ", on]
            where -> [where | [" AND ", on]]
          end

        return_old = Enum.map_intersperse(column_names, ", ", &["old.", &1])
        return_new = Enum.map_intersperse(column_names, ", ", &[source_alias, ".", &1])

        {returning, original_count} =
          case returning do
            [] ->
              {[" RETURNING ", return_old, ", ", return_new], 0}

            returning ->
              count = length(query.select.fields)
              {[returning | [",", return_old, ",", return_new]], count}
          end

        meta = %{
          returning:
            {original_count,
             Enum.map(field_names, &{:old, &1}) ++ Enum.map(field_names, &{:new, &1})},
          schema_meta: %{schema: schema, source: table, prefix: source_prefix},
          stack_id: stack_id
        }

        {:nocache, {{:update_all_sync, meta}, [cte, prefix, fields, join, where, returning]}}
      else
        # disable caching for update_all queries. otherwise any matching query with
        # the sandbox disabled will override the sandboxed version
        with {:cache, prepared} <- @adapter.prepare(:update_all, query) do
          {:nocache, prepared}
        end
      end
    end

    def prepare(:delete_all, query) do
      if stack_id = Phoenix.Sync.Sandbox.stack_id() do
        {table, schema, source_prefix} = elem(query.sources, 0)

        [cte, prefix, from, as, source_alias, join, where | returning] =
          Ecto.Adapters.Postgres.Connection.delete_all(query)

        field_names = schema.__schema__(:fields)
        column_names = Enum.map(field_names, &to_string(schema.__schema__(:field_source, &1)))

        return_old =
          Enum.map_intersperse(column_names, ", ", &[source_alias, ".", quote_name(&1)])

        {returning, original_count} =
          case returning do
            [] ->
              {[" RETURNING ", return_old], 0}

            returning ->
              count = length(query.select.fields)
              {[returning | [",", return_old]], count}
          end

        meta = %{
          returning: {original_count, Enum.map(field_names, &{:old, &1})},
          schema_meta: %{schema: schema, source: table, prefix: source_prefix},
          stack_id: stack_id
        }

        {:nocache,
         {{:delete_all_sync, meta},
          [cte, prefix, from, as, source_alias, join, where | returning]}}
      else
        # disable query caching for same reasons as update_all
        with {:cache, prepared} <- @adapter.prepare(:delete_all, query) do
          {:nocache, prepared}
        end
      end
    end

    def prepare(type, query) do
      @adapter.prepare(type, query)
    end

    @impl true
    def execute(
          adapter_meta,
          query_meta,
          {:nocache, {{:update_all_sync, meta}, _sql}} = query,
          params,
          opts
        ) do
      with {n, rows} <- @adapter.execute(adapter_meta, query_meta, query, params, opts) do
        {keep_count, columns} = meta.returning

        {return, changes} =
          Enum.reduce(rows, {[], []}, fn row, {return, emit} ->
            {return_cols, emit_cols} = Enum.split(row, keep_count)

            {new_row, old_row} =
              columns
              |> Enum.zip(emit_cols)
              |> Enum.reduce({[], []}, fn
                {{:new, col}, value}, {new_acc, old_acc} -> {[{col, value} | new_acc], old_acc}
                {{:old, col}, value}, {new_acc, old_acc} -> {new_acc, [{col, value} | old_acc]}
              end)

            {[return_cols | return], [{:update, meta.schema_meta, old_row, new_row} | emit]}
          end)

        Phoenix.Sync.Sandbox.Producer.emit_changes(meta.stack_id, Enum.reverse(changes))

        {n, if(keep_count > 0, do: Enum.reverse(return), else: nil)}
      end
    end

    def execute(
          adapter_meta,
          query_meta,
          {:nocache, {{:delete_all_sync, meta}, _sql}} = query,
          params,
          opts
        ) do
      with {n, rows} <- @adapter.execute(adapter_meta, query_meta, query, params, opts) do
        {keep_count, columns} = meta.returning

        {return, changes} =
          Enum.reduce(rows, {[], []}, fn row, {return, emit} ->
            {return_cols, emit_cols} = Enum.split(row, keep_count)

            old_row =
              columns
              |> Enum.zip(emit_cols)
              |> Enum.reduce([], fn
                {{:old, col}, value}, old_acc -> [{col, value} | old_acc]
              end)

            {[return_cols | return], [{:delete, meta.schema_meta, old_row} | emit]}
          end)

        Phoenix.Sync.Sandbox.Producer.emit_changes(meta.stack_id, Enum.reverse(changes))

        {n, if(keep_count > 0, do: Enum.reverse(return), else: nil)}
      end
    end

    def execute(adapter_meta, query_meta, query, params, opts) do
      @adapter.execute(adapter_meta, query_meta, query, params, opts)
    end

    @impl true
    defdelegate stream(adapter_meta, query_meta, query, params, opts), to: @adapter

    ## Schema

    @impl true
    defdelegate autogenerate(type), to: @adapter

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
      if stack_id = Phoenix.Sync.Sandbox.stack_id() do
        all_columns = schema_meta.schema.__schema__(:fields)

        with {count, rows} <-
               @adapter.insert_all(
                 adapter_meta,
                 schema_meta,
                 header,
                 rows,
                 on_conflict,
                 all_columns,
                 placeholders,
                 opts
               ) do
          inserted =
            Enum.map(rows, fn row -> {:insert, schema_meta, Enum.zip(all_columns, row)} end)

          Phoenix.Sync.Sandbox.Producer.emit_changes(stack_id, inserted)

          return_rows =
            case returning do
              [] ->
                nil

              columns ->
                # need to keep returning column order
                Enum.map(inserted, fn {:insert, _, insert} ->
                  Enum.map(columns, &Keyword.fetch!(insert, &1))
                end)
            end

          {count, return_rows}
        end
      else
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
    end

    @impl true
    def insert(
          adapter_meta,
          %{source: "schema_migrations"} = schema_meta,
          params,
          on_conflict,
          returning,
          opts
        ) do
      @adapter.insert(adapter_meta, schema_meta, params, on_conflict, returning, opts)
    end

    def insert(adapter_meta, schema_meta, params, on_conflict, returning, opts) do
      if stack_id = Phoenix.Sync.Sandbox.stack_id() do
        all_columns = schema_meta.schema.__schema__(:fields)

        with {:ok, inserted} <-
               @adapter.insert(adapter_meta, schema_meta, params, on_conflict, all_columns, opts) do
          Phoenix.Sync.Sandbox.Producer.emit_changes(stack_id, [
            {:insert, schema_meta, inserted}
          ])

          {:ok, take(inserted, returning)}
        end
      else
        @adapter.insert(
          adapter_meta,
          schema_meta,
          params,
          on_conflict,
          returning,
          opts
        )
      end
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

    @impl true
    def update(adapter_meta, schema_meta, fields, params, returning, opts) do
      if stack_id = Phoenix.Sync.Sandbox.stack_id() do
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

          Phoenix.Sync.Sandbox.Producer.emit_changes(stack_id, [
            {:update, schema_meta, old, new}
          ])

          {:ok, take(new, returning)}
        end
      else
        @adapter.update(adapter_meta, schema_meta, fields, params, returning, opts)
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
      if stack_id = Phoenix.Sync.Sandbox.stack_id() do
        all_columns = schema_meta.schema.__schema__(:fields)

        with {:ok, deleted} <-
               @adapter.delete(adapter_meta, schema_meta, params, all_columns, opts) do
          Phoenix.Sync.Sandbox.Producer.emit_changes(stack_id, [
            {:delete, schema_meta, deleted}
          ])

          {:ok, take(deleted, returning)}
        end
      else
        @adapter.delete(adapter_meta, schema_meta, params, returning, opts)
      end
    end

    # basically Keyword.take/2 but preserves the order of the columns
    defp take(row, columns) do
      Enum.map(columns, &{&1, Keyword.fetch!(row, &1)})
    end

    ## Transaction

    @impl true
    defdelegate transaction(meta, opts, fun), to: @adapter

    @impl true
    defdelegate in_transaction?(meta), to: @adapter

    @impl true
    defdelegate rollback(meta, value), to: @adapter

    ## Migration

    @impl true
    defdelegate execute_ddl(meta, definition, opts), to: @adapter

    @impl true
    defdelegate supports_ddl_transaction?(), to: @adapter

    @impl true
    defdelegate lock_for_migrations(meta, opts, fun), to: @adapter

    ## Storage

    @impl true
    defdelegate storage_up(opts), to: @adapter

    @impl true
    defdelegate storage_down(opts), to: @adapter

    @impl true
    defdelegate storage_status(opts), to: @adapter
  end
end
