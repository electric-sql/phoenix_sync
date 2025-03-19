# Rename to Phoenix.Sync.Mutation?
defmodule Phoenix.Sync.Write do
  @moduledoc """


  ```elixir
  Phoenix.Sync.Write.new(conn, [
    # just use the model's changeset to validate
    Todos.Todo,
    # use the `validate_write/2` function to map the write to a changeset
    {Todos.Todo, Todos.Todo.validate_write(&1, conn.params)},
    # map a table name to a changeset function
    {{"public", "todos"}, &Todos.Todo.changeset(&1, &2, conn)}
    {Todos.Todo,
      fetch: &Todos.fetch/1,
      insert: &Todos.Todo.insert_changeset(&1, &2, conn),
      update: &Todos.Todo.update_changeset(&1, &2, conn),
      delete: &Todos.Todo.delete_changeset(&1, &2, conn),
      effects: &Todos.Todo.apply_tx/2
    }
  ])
  ```
  actually, the examples show a single table in the data -- this is not a
  merged change log?
  """

  @type data() :: %{binary() => binary() | integer()}
  @type action() :: :insert | :update | :delete
  @actions [:insert, :update, :delete]

  @action_schema [
    type: :non_empty_keyword_list,
    keys: [changeset: [type: {:fun, 2}], before: [type: {:fun, 3}], after: [type: {:fun, 3}]]
  ]
  @schema NimbleOptions.new!(
            table: [type: {:or, [:string, {:list, :string}]}],
            load: [
              type: {:or, [{:fun, 1}, {:fun, 2}]},
              doc: """
              `load` should be a 1- or 2-arity function that accepts either the
              mutation data or an Ecto.Repo instance and the mutation data and returns
              the original row from the database or raises if not found.

              This function is only used for updates or deletes. For inserts,
              the `Ecto.Schema.__changeset__/0` function is used to create an empty
              schema struct.

              ## Examples

                  # load from a known Repo
                  load: fn %{"id" => id} -> MyApp.Repo.get!(Todos.Todo, id)

                  # load from the repo passed to `#{__MODULE__}.transaction/2`
                  load: fn repo, %{"id" => id} -> repo.get!(Todos.Todo, id)

              If not provided defaults to `Ecto.Repo.get_by!/2` using the
              table's schema module and its primary keys.
              """,
              type_spec:
                quote(
                  do: (Ecto.Repo.t(), data() -> Ecto.Schema.t()) | (data() -> Ecto.Schema.t())
                )
            ],
            accept: [type: {:list, {:in, [:insert, :update, :delete]}}],
            changeset: [type: {:or, [{:fun, 2}, {:fun, 3}]}],
            before: [
              type: {:fun, 4},
              doc: """
              `before` is an optional callback that allows for the pre-pending of
              operations to the `Ecto.Multi` representing a mutation transaction.

              If should be a 4-arity function.

              ### Arguments

              - `multi` an empty `%Ecto.Multi{}` instance that you should apply
                your actions to
              - `action` the operation action, one of `:insert`, `:update` or `:delete`
              - `changeset` the changeset representing the individual mutation operation
              - `data` the `Ecto.Multi` changes map

              The result should be the `Ecto.Multi` instance which will be
              [merged](`Ecto.Multi.merge/2`) with the one representing the mutation
              operation.
              """,
              type_spec:
                quote(
                  do: (Ecto.Multi.t(), action(), Ecto.Changeset.t(), Ecto.Multi.changes() ->
                         Ecto.Multi.t())
                )
            ],
            after: [
              type: {:fun, 4},
              doc: """
              `after` is an optional callback function that allows for the
              appending of operations to the `Ecto.Multi` representing a mutation
              transaction.

              See the docs for `:before` for the function signature and arguments.
              """,
              type_spec:
                quote(
                  do: (Ecto.Multi.t(), action(), Ecto.Changeset.t(), Ecto.Multi.changes() ->
                         Ecto.Multi.t())
                )
            ],
            insert: @action_schema,
            update: @action_schema,
            delete:
              Keyword.update!(
                @action_schema,
                :keys,
                &Keyword.put(&1, :changeset, type: {:or, [{:fun, 1}, {:fun, 2}]})
              )
          )

  defstruct mappings: %{}

  defmodule Relation do
    defstruct [:prefix, :table]

    defimpl Ecto.Queryable do
      def to_query(relation) do
        %Ecto.Query{
          from: %Ecto.Query.FromExpr{source: {relation.table, nil}, prefix: relation.prefix}
        }
      end
    end
  end

  def new(schema, config \\ [])

  def new(schema, opts) when is_atom(schema) do
    %__MODULE__{}
    |> allow(schema, opts)
  end

  def allow(write, schema, opts \\ [])

  def allow(%__MODULE__{} = write, schema, changeset_fun)
      when is_atom(schema) and is_function(changeset_fun) do
    allow(write, schema, changeset: changeset_fun)
  end

  def allow(%__MODULE__{} = write, schema, opts) when is_atom(schema) do
    {schema, table, pks} = validate_schema!(schema)

    config = NimbleOptions.validate!(opts, @schema)

    key = config[:table] || table
    load_fun = load_fun(schema, pks, opts)

    accept = Keyword.get(config, :accept, @actions) |> MapSet.new()

    table_config = %{
      table: table,
      pks: pks,
      accept: accept
    }

    table_config =
      Enum.reduce(@actions, table_config, fn action, table_config ->
        Map.put(
          table_config,
          action,
          action_config(schema, config, action, load: load_fun, table: key, pks: pks)
        )
      end)

    Map.update!(write, :mappings, &Map.put(&1, key, table_config))
  end

  defp validate_schema!(module) do
    if !Code.ensure_loaded?(module), do: raise(ArgumentError, message: "Unknown module #{module}")

    if !(function_exported?(module, :__changeset__, 0) &&
           function_exported?(module, :__schema__, 1)),
       do: raise(ArgumentError, message: "Not an Ecto.Schema module #{module}")

    table =
      if prefix = module.__schema__(:prefix),
        do: [prefix, module.__schema__(:source)],
        else: module.__schema__(:source)

    {module, table, module.__schema__(:primary_key)}
  end

  defp action_config(schema, config, action, extra) do
    changeset_fun =
      get_in(config, [action, :changeset]) || config[:changeset] || default_changeset!(schema) ||
        raise(ArgumentError, message: "No changeset/3 or changeset/2 defined for #{action}s")

    before_fun =
      get_in(config, [action, :before]) || config[:before] ||
        fn multi, _action, _changeset, _changes -> multi end

    after_fun =
      get_in(config, [action, :after]) || config[:after] ||
        fn multi, _action, _changeset, _changes -> multi end

    Map.merge(
      Map.new(extra),
      %{changeset: changeset_fun, before: before_fun, after: after_fun}
    )
  end

  defp load_fun(schema, pks, config) do
    load_fun =
      case config[:load] do
        nil ->
          fn repo, change ->
            key = Enum.map(pks, fn col -> {col, Map.fetch!(change, to_string(col))} end)
            repo.get_by!(schema, key)
          end

        fun when is_function(fun, 1) ->
          fn _repo, change ->
            fun.(change)
          end

        fun when is_function(fun, 2) ->
          fun

        _invalid ->
          raise(ArgumentError, message: "`load` should be a 1- or 2-arity function")
      end

    fn
      _repo, :insert, _change ->
        schema.__struct__()

      repo, action, change when action in [:update, :delete] ->
        load_fun.(repo, change)
    end
  end

  defp default_changeset!(schema) do
    cond do
      function_exported?(schema, :changeset, 3) -> &schema.changeset/3
      function_exported?(schema, :changeset, 2) -> &schema.changeset/2
      true -> nil
    end
  end

  def apply(%__MODULE__{} = write, changes) when is_list(changes) do
    changes
    |> Enum.with_index()
    |> Enum.reduce(
      start_multi(),
      &apply_change(&2, &1, write)
    )
  end

  @txid_name :__phoenix_sync_txid__

  defp start_multi do
    Ecto.Multi.new()
    |> Ecto.Multi.run(@txid_name, fn repo, _ ->
      with {:ok, %{rows: [[txid]]}} <- repo.query("SELECT txid_current() as txid") do
        {:ok, txid}
      end
    end)
  end

  defp apply_change(multi, {%{"type" => mutation_type} = change, n}, %__MODULE__{} = write)
       when is_map(change) do
    with {:ok, type} <- validate_type(mutation_type),
         {:ok, data} <- mutation_data(type, change),
         {:ok, actions} <- mutation_actions(change, write),
         {:ok, action} <- Map.fetch(actions, type),
         :ok <- validate_accept(type, actions.accept) do
      multi
      |> mutation_changeset(type, data, n, action)
      |> validate_pks(data, n, action)
      |> apply_before(type, n, action)
      |> apply_changeset(type, n, action)
      |> apply_after(type, n, action)
    else
      {:error, reason} ->
        Ecto.Multi.error(multi, {:invalid, n}, reason)
    end
  end

  defp mutation_changeset(multi, type, data, n, %{changeset: changeset_fun} = action) do
    {lookup_data, change_data} = data

    Ecto.Multi.run(multi, {:changeset, n}, fn repo, _ ->
      struct = action.load.(repo, type, lookup_data)

      cond do
        is_function(changeset_fun, 3) -> {:ok, changeset_fun.(struct, type, change_data)}
        is_function(changeset_fun, 2) -> {:ok, changeset_fun.(struct, change_data)}
        # delete changeset/validation functions can just look at the original
        is_function(changeset_fun, 1) -> {:ok, changeset_fun.(struct)}
        true -> raise "Invalid changeset_fun for #{inspect(action.table)} #{inspect(type)}"
      end
    end)
  end

  defp validate_pks(multi, {lookup, _}, n, action) do
    case Enum.reject(action.pks, &Map.has_key?(lookup, to_string(&1))) do
      [] ->
        multi

      keys ->
        Ecto.Multi.error(
          multi,
          {:error, n},
          "Mutation data is missing required primary keys: #{inspect(keys)}"
        )
    end
  end

  defp apply_before(multi, type, n, %{before: before_fun} = action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})

      cond do
        # per-action callback
        is_function(before_fun, 3) ->
          before_fun.(Ecto.Multi.new(), changeset, changes) |> validate_callback!(type, action)

        # global callback including action
        is_function(before_fun, 4) ->
          before_fun.(Ecto.Multi.new(), type, changeset, changes)
          |> validate_callback!(type, action)

        true ->
          raise "Invalid before_fun for #{inspect(action.table)} #{inspect(type)}"
      end
    end)
  end

  defp apply_after(multi, type, n, %{after: after_fun} = action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})

      cond do
        # per-action callback
        is_function(after_fun, 3) ->
          after_fun.(Ecto.Multi.new(), changeset, changes) |> validate_callback!(type, action)

        # global callback including action
        is_function(after_fun, 4) ->
          after_fun.(Ecto.Multi.new(), type, changeset, changes)
          |> validate_callback!(type, action)

        true ->
          raise "Invalid after_fun for #{inspect(action.table)} #{inspect(type)}"
      end
    end)
  end

  defp apply_changeset(multi, :insert, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})
      Ecto.Multi.insert(Ecto.Multi.new(), {:insert, n}, changeset)
    end)
  end

  defp apply_changeset(multi, :update, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})
      Ecto.Multi.update(Ecto.Multi.new(), {:update, n}, changeset)
    end)
  end

  defp apply_changeset(multi, :delete, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})
      Ecto.Multi.delete(Ecto.Multi.new(), {:delete, n}, changeset)
    end)
  end

  defp validate_callback!(%Ecto.Multi{} = multi, _type, _action), do: multi

  defp validate_callback!(value, type, action),
    do:
      raise(ArgumentError,
        message:
          "Invalid return type #{inspect(value)} for #{type} into #{inspect(action.table)}. Expected %Ecto.Multi{}"
      )

  defp mutation_data(:insert, %{"modified" => data}), do: {:ok, {data, data}}

  defp mutation_data(:update, %{"changes" => changes, "original" => original}),
    do: {:ok, {original, changes}}

  defp mutation_data(:delete, %{"original" => original}), do: {:ok, {original, original}}

  defp mutation_data(action, mutation),
    do: {:error, "Invalid #{action} mutation, no data keys: #{inspect(Map.keys(mutation))}"}

  defp mutation_actions(%{"syncMetadata" => %{"relation" => [prefix, name] = relation}}, write)
       when is_binary(name) and is_binary(prefix) do
    case write.mappings do
      %{^relation => actions} ->
        {:ok, actions}

      %{^name => actions} ->
        {:ok, actions}

      _ ->
        {:error, "No configuration for writes to table #{inspect(name)}"}
    end
  end

  defp mutation_actions(%{"syncMetadata" => %{"relation" => name}}, write) when is_binary(name) do
    case write.mappings do
      %{^name => actions} ->
        {:ok, actions}

      mappings ->
        case Enum.filter(Map.keys(mappings), &match?([_, ^name], &1)) do
          [] ->
            {:error, "No configuration for writes to table #{inspect(name)}"}

          [key] ->
            {:ok, Map.fetch!(write.mappings, key)}

          [_ | _] = keys ->
            {:error,
             "Multiple matches for relation #{inspect(name)}: #{inspect(keys)}. Please pass full `[\"schema\", \"name\"]` relation in mutation data"}
        end
    end
  end

  defp validate_accept(type, allowed) do
    if type in allowed do
      :ok
    else
      {:error,
       "Action #{inspect(type)} not in :accept list: #{MapSet.to_list(allowed) |> inspect()}"}
    end
  end

  defp validate_type("insert"), do: {:ok, :insert}
  defp validate_type("update"), do: {:ok, :update}
  defp validate_type("delete"), do: {:ok, :delete}

  defp validate_type(type),
    do:
      {:error,
       "Invalid action #{inspect(type)} expected one of #{inspect(~w(insert update delete))}"}

  def transaction(%Ecto.Multi{} = multi, repo) when is_atom(repo) do
    with {:ok, changes} <- repo.transaction(multi) do
      {txid, changes} = Map.pop!(changes, @txid_name)
      {:ok, txid, changes}
    end
  end
end
