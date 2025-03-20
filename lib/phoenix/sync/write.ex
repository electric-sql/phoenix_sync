defmodule Phoenix.Sync.Write do
  @moduledoc """
  """

  defmodule Mutation do
    @moduledoc false
    defstruct [:type, :relation, :data, :changes]

    @type t() :: %__MODULE__{
            type: :insert | :update | :delete,
            relation: binary() | [binary(), ...],
            data: map(),
            changes: map()
          }

    @doc """
    Takes data from a mutation and validates it before returning a struct.

    ## Parameters

    - `action` one of `"insert"`, `"update"` or `"delete"`.
    - `table` the client table name for the write. Can either be a plain string
      name or a list with `["schema", "table"]`.
    - `data` the original values (see "Updates vs Inserts vs Deletes")
    - `changes` any updates to apply (see "Updates vs Inserts vs Deletes")

    ## Updates vs Inserts vs Deletes

    The `Mutation` struct has two value fields, `data` and `changes`.

    `data` represents what's already in the database, and `changes` what's
    going to be written over the top of this.

    For `insert` operations, `data` is ignored so the new values for the
    inserted row should be in `changes`.

    For `deletes`, `changes` is ignored and `data` should contain the row
    specification to delete. This needn't be the full row, but must contain
    values for all the primary keys for the table.

    For `updates`, `data` should contain the original row value and `changes`
    the changed fields.

    These fields map to the arguments `Ecto.Changeset.change/2` and
    `Ecto.Changeset.cast/4` functions, `data` is used to populate the first
    argument of these functions and `changes` the second.
    """
    @spec new(binary(), binary() | [binary(), ...], map() | nil, map() | nil) ::
            {:ok, t()} | {:error, String.t()}
    def new(action, table, data, changes) do
      with {:ok, type} <- validate_action(action),
           {:ok, relation} <- validate_table(table),
           {:ok, data} <- validate_data(data, type),
           {:ok, changes} <- validate_changes(changes, type) do
        {:ok, %__MODULE__{type: type, relation: relation, data: data, changes: changes}}
      end
    end

    @doc """
    Parses the mutation data returned from
    [KyleAMathews/optimistic](https://github.com/KyleAMathews/optimistic).
    """
    @spec default(%{binary() => binary() | map()}) :: {:ok, t()} | {:error, String.t()}
    def default(%{"type" => type} = m) do
      {data, changes} =
        case type do
          # for inserts we don't use the original data, just the changes
          "insert" -> {%{}, m["modified"]}
          "update" -> {m["original"], m["changes"]}
          "delete" -> {m["original"], %{}}
        end

      new(type, get_in(m, ["syncMetadata", "relation"]), data, changes)
    end

    defp validate_action("insert"), do: {:ok, :insert}
    defp validate_action("update"), do: {:ok, :update}
    defp validate_action("delete"), do: {:ok, :delete}

    defp validate_action(type),
      do:
        {:error,
         "Invalid action #{inspect(type)} expected one of #{inspect(~w(insert update delete))}"}

    defp validate_table(name) when is_binary(name), do: {:ok, name}

    defp validate_table([prefix, name] = relation) when is_binary(name) and is_binary(prefix),
      do: {:ok, relation}

    defp validate_table(table), do: {:error, "Invalid table: #{inspect(table)}"}

    defp validate_data(_, :insert), do: {:ok, %{}}
    defp validate_data(%{} = data, _), do: {:ok, data}

    defp validate_data(data, _type),
      do: {:error, "Invalid data expected map got #{inspect(data)}"}

    defp validate_changes(_, :delete), do: {:ok, %{}}
    defp validate_changes(%{} = changes, _insert_or_update), do: {:ok, changes}

    defp validate_changes(changes, _insert_or_update),
      do: {:error, "Invalid changes for update. Expected map got #{inspect(changes)}"}
  end

  @type data() :: %{binary() => binary() | integer()}
  @type action() :: :insert | :update | :delete
  @actions [:insert, :update, :delete]

  @action_schema [
    type: :non_empty_keyword_list,
    keys: [
      changeset: [type: {:or, [{:fun, 2}, :mfa]}],
      before: [type: {:or, [{:fun, 3}, :mfa]}],
      after: [type: {:or, [{:fun, 3}, :mfa]}]
    ]
  ]

  @mutator_schema NimbleOptions.new!(
                    parser: [
                      type: {:or, [{:fun, 1}, :mfa]},
                      doc: """
                      `parser` should be a function that takes a mutation event
                      from the client as a map (most probably parsed from JSON) and returns a
                      `%Mutation{}` struct.

                      It should use `#{Mutation}.new/4` to validate the mutation data.
                      """
                    ]
                  )

  @allow_schema NimbleOptions.new!(
                  table: [
                    type: {:or, [:string, {:list, :string}]},
                    doc: """
                    Override the table name of the `Ecto.Schema` struct to
                    allow for mapping between table names on the client and within Postgres.
                    """
                  ],
                  load: [
                    type: {:or, [{:fun, 1}, {:fun, 2}, :mfa]},
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
                        do:
                          (Ecto.Repo.t(), data() -> Ecto.Schema.t()) | (data() -> Ecto.Schema.t())
                      )
                  ],
                  accept: [
                    type: {:list, {:in, @actions}},
                    doc: """
                    A list of actions to accept. Defaults to accepting all operations, #{inspect(@actions)}.
                    """
                  ],
                  changeset: [
                    type: {:or, [{:fun, 3}, :mfa]},
                    doc: """
                    `changeset` should be a 3-arity function that returns
                    an `Ecto.Changeset` for a given mutation.

                    ## Params
                    #
                    - `data` an Ecto.Schema struct matching the one used when calling `allow/2`
                    - `action` the operation action, one of `:insert`, `:update` or `:delete`
                    - `changes` a map of changes to apply to the `data`.

                    At absolute minimum, this should call
                    `Ecto.Changeset.cast/3` to validate the proposed data:

                        def my_changeset(data, _action, changes) do
                          Ecto.Changeset.cast(data, changes, @permitted_columns)
                        end

                    But it's vitally important to remember that the `data` and
                    `changes` are **untrusted** values.

                    Unlike standard uses of `Ecto.Changeset` functions, where
                    authentication and authorization concerns are usually handled at the
                    router/controller level, mutation data **must** be authenticated within the
                    changeset as well as validated.

                    This is significantly more complex than just data
                    validataion. For this reason we do not recommend using the standard
                    generated `changeset/2` function.

                    ## Example

                    For example, we want to accept changes to a todo list from
                    remote users but every user has their own todos, and must be prevented
                    from modifying todos belonging to other users.

                    Within our controller we might do:

                    ```elixir
                    # our Plug pipeline verifies that the `user_id` here matches
                    # the logged-in user
                    def mutations(conn, %{"user_id" => user_id} = params) do
                      {:ok, txid, _changes} =
                        Todos.Todo
                        |> Phoenix.Sync.Write.mutator(changeset: &todo_changeset(&1, &2, &3, user_id))
                        |> Phoenix.Sync.Write.apply(params["_json"] || [])
                        |> Phoenix.Sync.Write.transaction(MyApp.Repo)

                      # return the txid back to the client so it knows when its writes have been accepted
                      render(conn, :mutations, txid: txid)
                    end

                    # for inserts we only have to validate that the user_id in the changes
                    # matches the authenticated user_id
                    defp todo_changeset(todo, :insert, changes, user_id) do
                      todo
                      |> Ecto.Changeset.cast(changes, [:id, :user_id, :title, :completed])
                      |> Ecto.Changeset.validate_required([:id, :title, :user_id])
                      |> Ecto.Changeset.validate_change(:user_id, fn :user_id, id ->
                        if id != user_id do
                          [user_id: "is invalid"]
                        else
                          []
                        end
                      end)
                    end

                    # for updates we have to validate both the original user_id
                    # **and** the final value
                    defp todo_changeset(%{user_id: user_id} = todo, :update, changes, user_id) do
                      todo
                      |> Ecto.Changeset.cast(changes, [:id, :user_id, :title, :completed])
                      |> Ecto.Changeset.validate_required([:id, :title, :user_id])
                      |> Ecto.Changeset.validate_change(:user_id, fn :user_id, id ->
                        if id != user_id do
                          [user_id: "is invalid"]
                        else
                          []
                        end
                      end)
                    end

                    # for deletes only the original value matters
                    defp todo_changeset(%{user_id: user_id} = todo, :delete, _changes, user_id) do
                      todo
                    end

                    # the fallback for when the original todo does not belong to the user
                    defp todo_changeset(todo, _, changes, _user_id) do
                      todo
                      |> Ecto.Changeset.change(%{})
                      |> Ecto.Changeset.add_error(:user_id, "does not match the current user")
                    end
                    ```
                    """,
                    type_spec:
                      quote(do: (Ecto.Schema.t(), action(), data() -> Ecto.Changeset.t()))
                  ],
                  before: [
                    type: {:or, [{:fun, 4}, :mfa]},
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
                    type: {:or, [{:fun, 4}, :mfa]},
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
                  insert:
                    Keyword.put(@action_schema, :doc, """
                    Callbacks for validating and modifying `insert` operations.

                    Accepts definitions for the `changeset`, `before` and
                    `after` functions for `insert` operations that will override the
                    top-level equivalents.

                    The only difference with these callback functions is that
                    the `action` parameter is redundant and therefore not passed.
                    """),
                  update:
                    Keyword.put(@action_schema, :doc, """
                    Callbacks for validating and modifying `update` operations.
                    See the documentation for `insert`.
                    """),
                  delete:
                    Keyword.update!(
                      @action_schema,
                      :keys,
                      &Keyword.put(&1, :changeset, type: {:or, [{:fun, 1}, {:fun, 2}, :mfa]})
                    )
                    |> Keyword.put(:doc, """
                    Callbacks for validating and modifying `delete` operations.
                    See the documentation for `insert`.
                    """)
                )

  defstruct parser: &Mutation.default/1, mappings: %{}

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

  def mutator do
    %__MODULE__{}
  end

  def mutator(opts) when is_list(opts) do
    config = NimbleOptions.validate!(opts, @mutator_schema)
    parser = Keyword.get(config, :parser, &Mutation.default/1)
    %__MODULE__{parser: parser}
  end

  def mutator(schema) when is_atom(schema) do
    mutator(schema, [])
  end

  def mutator(schema, changeset_fun) when is_atom(schema) and is_function(changeset_fun) do
    %__MODULE__{}
    |> allow(schema, changeset: changeset_fun)
  end

  def mutator(schema, opts) when is_atom(schema) and is_list(opts) do
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

    config = NimbleOptions.validate!(opts, @allow_schema)

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

        {m, f, a} = mfa when is_atom(m) and is_atom(f) and is_list(a) ->
          mfa

        _invalid ->
          raise(ArgumentError, message: "`load` should be a 1- or 2-arity function")
      end

    fn
      _repo, :insert, _change ->
        schema.__struct__()

      repo, action, change when action in [:update, :delete] ->
        # need to do this function lookup at runtime
        case load_fun do
          fun when is_function(fun, 2) ->
            fun.(repo, change)

          {m, f, a} ->
            l = length(a)

            cond do
              function_exported?(m, f, l + 2) ->
                apply(m, f, [repo, change | a])

              function_exported?(m, f, l + 1) ->
                apply(m, f, [change | a])
            end
        end
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

  defp apply_change(multi, {change, n}, %__MODULE__{} = write)
       when is_map(change) do
    with {:ok, %Mutation{type: type} = mutation} <- parse_change(change, write),
         {:ok, actions} <- mutation_actions(mutation, write),
         {:ok, action} <- Map.fetch(actions, type),
         :ok <- validate_accept(type, actions.accept) do
      multi
      |> mutation_changeset(mutation, n, action)
      |> validate_pks(mutation, n, action)
      |> apply_before(mutation, n, action)
      |> apply_changeset(mutation, n, action)
      |> apply_after(mutation, n, action)
    else
      {:error, reason} ->
        Ecto.Multi.error(multi, {:invalid, n}, reason)
    end
  end

  defp parse_change(%{} = change, %__MODULE__{parser: parser}) do
    case parser do
      fun when is_function(fun, 1) ->
        fun.(change)

      {m, f, a} ->
        apply(m, f, [change | a])
    end
  end

  defp mutation_changeset(multi, %Mutation{} = mutation, n, %{changeset: changeset_fun} = action) do
    %{type: type, data: lookup_data, changes: change_data} = mutation

    Ecto.Multi.run(multi, {:changeset, n}, fn repo, _ ->
      struct = action.load.(repo, type, lookup_data)

      case changeset_fun do
        fun3 when is_function(fun3, 3) ->
          {:ok, fun3.(struct, type, change_data)}

        fun2 when is_function(fun2, 2) ->
          {:ok, fun2.(struct, change_data)}

        # delete changeset/validation functions can just look at the original
        fun1 when is_function(fun1, 1) ->
          {:ok, fun1.(struct)}

        {m, f, a} ->
          l = length(a)

          cond do
            function_exported?(m, f, l + 3) ->
              {:ok, apply(m, f, [struct, type, change_data | a])}

            function_exported?(m, f, l + 2) ->
              {:ok, apply(m, f, [struct, change_data | a])}

            function_exported?(m, f, l + 1) ->
              {:ok, apply(m, f, [struct | a])}

            true ->
              raise "Invalid changeset_fun for #{inspect(action.table)} #{inspect(type)}: #{inspect({m, f, a})}"
          end

        _ ->
          raise "Invalid changeset_fun for #{inspect(action.table)} #{inspect(type)}"
      end
    end)
  end

  defp validate_pks(multi, %Mutation{type: :insert, changes: changes}, n, action) do
    do_validate_pks(multi, action.pks, changes, n)
  end

  defp validate_pks(multi, %Mutation{data: lookup}, n, action) do
    do_validate_pks(multi, action.pks, lookup, n)
  end

  defp do_validate_pks(multi, pks, data, n) do
    case Enum.reject(pks, &Map.has_key?(data, to_string(&1))) do
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

  defp apply_before(multi, mutation, n, %{before: before_fun} = action) do
    apply_hook(multi, mutation, n, before_fun, action)
  end

  defp apply_after(multi, mutation, n, %{after: after_fun} = action) do
    apply_hook(multi, mutation, n, after_fun, action)
  end

  defp apply_hook(multi, mutation, n, hook_fun, action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})

      case hook_fun do
        # per-action callback
        fun3 when is_function(fun3, 3) ->
          fun3.(Ecto.Multi.new(), changeset, changes)

        # global callback including action
        fun4 when is_function(fun4, 4) ->
          fun4.(Ecto.Multi.new(), mutation.type, changeset, changes)

        {m, f, a} ->
          l = length(a)

          cond do
            function_exported?(m, f, l + 3) ->
              apply(m, f, [Ecto.Multi.new(), changeset, changes | a])

            function_exported?(m, f, l + 4) ->
              apply(m, f, [Ecto.Multi.new(), mutation.type, changeset, changes | a])

            true ->
              raise "Invalid after_fun for #{inspect(action.table)} #{inspect(mutation.type)}: #{inspect({m, f, a})}"
          end

        _ ->
          raise "Invalid after_fun for #{inspect(action.table)} #{inspect(mutation.type)}"
      end
      |> validate_callback!(mutation.type, action)
    end)
  end

  defp apply_changeset(multi, %Mutation{type: :insert}, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})
      Ecto.Multi.insert(Ecto.Multi.new(), {:insert, n}, changeset)
    end)
  end

  defp apply_changeset(multi, %Mutation{type: :update}, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = Map.fetch!(changes, {:changeset, n})
      Ecto.Multi.update(Ecto.Multi.new(), {:update, n}, changeset)
    end)
  end

  defp apply_changeset(multi, %Mutation{type: :delete}, n, _action) do
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

  defp mutation_actions(%Mutation{relation: [prefix, name] = relation}, write)
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

  defp mutation_actions(%Mutation{relation: name}, write) when is_binary(name) do
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

  def transaction(%Ecto.Multi{} = multi, repo) when is_atom(repo) do
    with {:ok, changes} <- repo.transaction(multi) do
      {txid, changes} = Map.pop!(changes, @txid_name)
      {:ok, txid, changes}
    end
  end
end
