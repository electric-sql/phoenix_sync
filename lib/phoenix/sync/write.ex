defmodule Phoenix.Sync.Write do
  @moduledoc """
  Provides [optimistic write](https://electric-sql.com/docs/guides/writes)
  support for Phoenix- or Plug-based apps.

  Local writes are performed on the local database using
  [TanStack/optimistic](https://github.com/TanStack/optimistic) which POSTs
  transactions to the Elixir/Phoenix application.

  A transaction is a list of "mutations", a series of `INSERT`, `UPDATE` and
  `DELETE` operations. These operations are then written to the server's
  database via the functions in this module and the transaction id of this
  write is returned to the client.

  When the client receives this transaction id back through it's Electric sync
  stream then the client knows that it's up-to-date with the server.

  ## Client Code

  The client needs to convert local writes into a series of transactional
  mutation lists, that is a series of `insert`, `update` and `delete`
  operations (see `Phoenix.Sync.Write.Mutation` for details of the required
  data).

  It then uses some kind of HTTP request (e.g. [`HTTP
  POST`](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API) or a
  Websocket/[Phoenix channel](https://hexdocs.pm/phoenix/channels.html) to send
  each transaction to your Phoenix controller.

  See [TanStack/optimistic](https://github.com/TanStack/optimistic) for more information.


  ## Server code

  The `todos` table is represented by the `Ecto.Schema` module `Todo`.

  ```elixir
  defmodule Todo do
    use Ecto.Schema

    schema "todos" do
      field :title, :string
      field :completed, :boolean, default: false
    end

    def changeset(todo, data) do
      todo
      |> cast(data, [:id, :title, :completed])
      |> validate_required([:id, :title])
    end
  end
  ```

  Our router maps the `/mutations` path to our `MutationsController.update/2`
  function.

  ```elixir
  defmodule MutationsController do
    use Phoenix.Controller, formats: [:json]

    alias Phoenix.Sync.Write

    def update(conn, %{transaction: transaction} = _params) do
      {:ok, txid, _changes} =
        Todo
        |> Write.allow()
        |> Write.apply(transaction)
        |> Write.transaction(Repo)
      render(conn, :update, txid: txid)
    end
  end
  ```

  - `allow/1` creates a write configuration that allows writes from the `todos`
    table. You can allow writes from any number of tables by repeatedly calling
    `allow/3` with the matching `Ecto.Schema`.

        writer =
          Write.new()
          |> Write.allow(Todo)
          |> Write.allow(Note)
          |> Write.allow(Event)

  - `apply/2` transforms the list of mutations into an `Ecto.Multi` instance
    containing the set of actions to perform to validate and persist this client
    transaction to the server database.

  - `transaction/2` calls `c:Ecto.Repo.transaction/2` with the `Ecto.Multi`
    from the `apply/2` step and extracts the resulting transaction id to return
    to the client as the JSON `{"txid":...}`.

  Because `Phoenix.Sync.Write` leverages `Ecto.Multi` to do the work of
  applying changes and managing errors, you're also free to extend the actions
  that are performed with every transaction using `before` and `after`
  callbacks configured per-table or per-table per-action (insert, update,
  delete). See `allow/3` for more information on the configuration options
  for each table.

  Because the result of `apply/2` is an `Ecto.Multi` instance you can also just append operations using the normal `Ecto.Multi` functions:

      {:ok, txid, _changes} =
        Todo
        |> Write.allow()
        |> Write.apply(transaction)
        |> Ecto.Multi.insert(:my_action, %Event{})
        |> Write.transaction(Repo)
  """

  alias __MODULE__.Mutation

  @operation_options [
    changeset: [
      type: {:or, [{:fun, 2}, :mfa]},
      doc: """
      A 2-arity function that returns a changeset for the given mutation data.
      """
    ],
    before: [
      type: {:or, [{:fun, 3}, :mfa]},
      doc: """
      An optional callback that allows for the pre-pending of operations to the `Ecto.Multi`.
      """
    ],
    after: [
      type: {:or, [{:fun, 3}, :mfa]},
      doc: """
      An optional callback that allows for the appending of operations to the `Ecto.Multi`.
      """
    ]
  ]
  @operation_options_schema NimbleOptions.new!(@operation_options)

  @type data() :: %{binary() => any()}
  @type mutation() :: %{required(binary()) => any()}
  @type operation() :: :insert | :update | :delete
  @operations [:insert, :update, :delete]
  @type txid() :: integer()

  @type operation_opts() :: unquote([NimbleOptions.option_typespec(@operation_options_schema)])

  @operation_schema [
    type: :non_empty_keyword_list,
    keys: @operation_options,
    doc: NimbleOptions.docs(NimbleOptions.new!(@operation_options)),
    type_doc: "`t:operation_opts/0`"
  ]

  @writer_schema NimbleOptions.new!(
                   parser: [
                     type: {:or, [{:fun, 1}, :mfa]},
                     doc: """
                     `parser` should be a function that takes a mutation event
                     from the client as a map (most probably parsed from JSON) and returns a
                     `%Mutation{}` struct.

                     It should use `#{Mutation}.new/4` to validate the mutation data.
                     """,
                     type_spec: quote(do: (mutation() -> Mutation.t()))
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
                    A 1- or 2-arity function that accepts either the
                    mutation data or an `Ecto.Repo` instance and the mutation data and returns
                    the original row from the database.

                    Valid return values are:

                    - `struct()` - an `Ecto.Schema` struct, that must match the module passed to `allow/3`
                    - `{:ok, struct()}` - as above but wrapped in an `:ok` tuple
                    - `nil` - if no row matches the search criteria, or
                    - `{:error, String.t()}` - as `nil` but with a custom error string

                    This function is only used for updates or deletes. For
                    inserts, the `__struct__/0` function defined by `Ecto.Schema` is used to
                    create an empty schema struct.

                    ## Examples

                        # load from a known Repo
                        load: fn %{"id" => id} -> MyApp.Repo.get(Todos.Todo, id)

                        # load from the repo passed to `#{__MODULE__}.transaction/2`
                        load: fn repo, %{"id" => id} -> repo.get(Todos.Todo, id)

                    If not provided defaults to `c:Ecto.Repo.get_by/3` using the
                    table's schema module and its primary keys.
                    """,
                    type_spec:
                      quote(
                        do:
                          (Ecto.Repo.t(), data() ->
                             Ecto.Schema.t() | {:ok, Ecto.Schema.t()} | nil | {:error, String.t()})
                          | (data() ->
                               Ecto.Schema.t()
                               | {:ok, Ecto.Schema.t()}
                               | nil
                               | {:error, String.t()})
                      )
                  ],
                  accept: [
                    type: {:list, {:in, @operations}},
                    doc: """
                    A list of actions to accept. Defaults to accepting all operations, `#{inspect(@operations)}`.
                    """
                  ],
                  changeset: [
                    type: {:or, [{:fun, 3}, :mfa]},
                    doc: """
                    `changeset` should be a 3-arity function that returns
                    an `Ecto.Changeset` for a given mutation.

                    ### Callback params

                    - `data` an Ecto.Schema struct matching the one used when
                      calling `allow/2` returned from the `load` function.
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
                    router/controller level, mutation data **must** be **authenticated** within the
                    changeset as well as **validated**.

                    This is significantly more complex than just data
                    validataion. For this reason we do not recommend using the standard
                    generated `changeset/2` function.

                    ## Example

                    For example, we want to accept changes to a todo list from
                    remote users but every user has their own todos, and must be prevented
                    from modifying todos belonging to other users.

                    Within our controller we might do:

                    ```elixir
                    # Our Plug pipeline verifies that the `user_id` here matches
                    # the logged-in user
                    def mutations(%Plug.Conn{} = conn, %{"user_id" => user_id} = params) do
                      {:ok, txid, _changes} =
                        Todos.Todo
                        |> Phoenix.Sync.Write.new(
                          load: &todo_fetch(&1, user_id),
                          changeset: &todo_changeset(&1, &2, &3, user_id)
                        )
                        |> Phoenix.Sync.Write.apply(params["_json"] || [])
                        |> Phoenix.Sync.Write.transaction(MyApp.Repo)

                      # return the txid back to the client so it knows when its writes have been accepted
                      render(conn, :mutations, txid: txid)
                    end

                    # To validate that each todo being modified belongs to the current user,
                    # use a load function that includes the user_id in the query
                    defp todo_fetch(%{"id" => id}, user_id) do
                      from(t in Todos.Todo, where: t.id == ^id and t.user_id == ^user_id)
                      |> MyApp.Repo.one()
                      |> case do
                        nil -> {:error, "No todo id: \#{inspect(id)} found for user \#{inspect(user_id)}"
                        %Todos.Todo{} = todo -> {:ok, todo}
                      end
                    end

                    # For inserts we have to validate that the user_id in the changes
                    # matches the authenticated user_id
                    defp todo_changeset(todo, :insert, changes, user_id) do
                      todo
                      |> Ecto.Changeset.cast(changes, [:id, :user_id, :title, :completed])
                      |> Ecto.Changeset.validate_required([:id, :title, :user_id])
                      |> Ecto.Changeset.validate_change(:user_id, fn :user_id, insert_user_id ->
                        if insert_user_id != user_id do
                          [user_id: "is invalid"]
                        else
                          []
                        end
                      end)
                    end

                    # For updates and deletes, we have validated that the
                    # original row belongs to the current user in `todo_fetch/2`.
                    #
                    # Because we don't want the user to be able to move a todo from themselves
                    # to another user, we simply disallow changes to `user_id` by removing
                    # it from the list of permitted columns in the call to `cast/3`
                    defp todo_changeset(todo, :update, changes, _user_id) do
                      todo
                      |> Ecto.Changeset.cast(changes, [:id, :title, :completed])
                      |> Ecto.Changeset.validate_required([:id, :title])
                    end

                    # For deletes only the original value matters since we aren't updating
                    # any column values
                    defp todo_changeset(todo, :delete, _changes, _user_id) do
                      todo
                    end
                    ```
                    """,
                    type_spec:
                      quote(do: (Ecto.Schema.t(), operation(), data() -> Ecto.Changeset.t()))
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

                    Because every action in an `Ecto.Multi` must have a unique key, we
                    recommend using the primary key of the row being modified as part of the
                    `name` in any additional operations to avoid conflicts.

                        def my_before(multi, :insert, changeset, _changes) do
                          id = Ecto.Changeset.fetch_field!(changeset, :id)
                          Ecto.Multi.insert(multi, {:event_insert, id}, %Event{todo_id: id})
                        end
                    """,
                    type_spec:
                      quote(
                        do: (Ecto.Multi.t(),
                             operation(),
                             Ecto.Changeset.t(),
                             Ecto.Multi.changes() ->
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
                        do: (Ecto.Multi.t(),
                             operation(),
                             Ecto.Changeset.t(),
                             Ecto.Multi.changes() ->
                               Ecto.Multi.t())
                      )
                  ],
                  insert:
                    Keyword.put(@operation_schema, :doc, """
                    Callbacks for validating and modifying `insert` operations.

                    Accepts definitions for the `changeset`, `before` and
                    `after` functions for `insert` operations that will override the
                    top-level equivalents.

                    See the documentation for `allow/3`.

                    The only difference with these callback functions is that
                    the `action` parameter is redundant and therefore not passed.
                    """),
                  update:
                    Keyword.put(@operation_schema, :doc, """
                    Callbacks for validating and modifying `update` operations.
                    See the documentation for `insert`.
                    """),
                  delete:
                    Keyword.update!(
                      @operation_schema,
                      :keys,
                      &Keyword.put(&1, :changeset, type: {:or, [{:fun, 1}, {:fun, 2}, :mfa]})
                    )
                    |> Keyword.put(:doc, """
                    Callbacks for validating and modifying `delete` operations.
                    See the documentation for `insert`.
                    """)
                )

  defstruct parser: &Mutation.tanstack/1, mappings: %{}

  @type writer_opts() :: [unquote(NimbleOptions.option_typespec(@writer_schema))]
  @type allow_opts() :: [unquote(NimbleOptions.option_typespec(@allow_schema))]

  @type writer() :: %__MODULE__{}

  @doc """
  Create a new empty writer with the given global options.

  Supported options:

  #{NimbleOptions.docs(@writer_schema)}

  Empty writers will reject writes to any tables. You should configure writes
  to the permitted tables by calling `allow/3`.
  """
  def new(opts \\ [])

  @spec new(writer_opts()) :: writer()
  def new(opts) when is_list(opts) do
    config = NimbleOptions.validate!(opts, @writer_schema)
    parser = Keyword.get(config, :parser, &Mutation.tanstack/1)
    %__MODULE__{parser: parser}
  end

  @doc """
  Create a writer config that allows writes to the given `Ecto.Schema` module.

  This is equivalent to:

      Phoenix.Sync.Write.new()
      |> Phoenix.Sync.Write.allow(MyApp.SchemaModule)
  """
  @spec allow(module()) :: writer()
  def allow(schema) when is_atom(schema) do
    allow(new(), schema, [])
  end

  @doc """
  Create a single-table writer configuration.

  Shortcut to:

      Phoenix.Sync.Write.new()
      |> Phoenix.Sync.Write.allow(MyApp.SchemaModule, opts)

  `opts` can be a list of options, see `allow/3`, or a 2- or 3-arity changeset
  function.

  ## Examples

      # allow writes to the Todo table using the `MyApp.Todos.Todo.changeset/2` function
      Phoenix.Sync.Write.allow(MyApp.Todos.Todo)

      # allow writes to the Todo table but use `MyApp.Todos.Todo.mutation_changeset/3` to validate
      # operations
      Phoenix.Sync.Write.allow(MyApp.Todos.Todo, &MyApp.Todos.Todo.mutation_changeset/3)

      # A more complex configuration adding an `after` callback to inserts
      # and using a custom query to load the original database value.
      Phoenix.Sync.Write.allow(
        MyApp.Todo,
        load: &MyApp.Todos.get_for_mutation/1,
        changeset: &MyApp.Todos.Todo.mutation_changeset/3,
        insert: [
          after: &MyApp.Todos.after_insert_mutation/3
        ]
      )
  """
  def allow(schema, opts) when is_atom(schema) and is_list(opts) do
    allow(new(), schema, opts)
  end

  def allow(schema, changeset_fun)
      when is_atom(schema) and is_function(changeset_fun) do
    allow(schema, changeset: changeset_fun)
  end

  @doc """
  Allow writes to the given `Ecto.Schema`.

  Supported options:

  #{NimbleOptions.docs(@allow_schema)}
  """
  @spec allow(writer(), module(), allow_opts()) :: writer()
  def allow(writer, schema, opts \\ [])

  def allow(%__MODULE__{} = writer, schema, changeset_fun)
      when is_atom(schema) and is_function(changeset_fun) do
    allow(writer, schema, changeset: changeset_fun)
  end

  def allow(%__MODULE__{} = write, schema, opts) when is_atom(schema) do
    {schema, table, pks} = validate_schema!(schema)

    config = NimbleOptions.validate!(opts, @allow_schema)

    key = config[:table] || table
    load_fun = load_fun(schema, pks, opts)

    accept = Keyword.get(config, :accept, @operations) |> MapSet.new()

    table_config = %{
      table: table,
      pks: pks,
      accept: accept
    }

    table_config =
      Enum.reduce(@operations, table_config, fn action, table_config ->
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

    # nil-hooks are just ignored
    before_fun = get_in(config, [action, :before]) || config[:before]
    after_fun = get_in(config, [action, :after]) || config[:after]

    Map.merge(
      Map.new(extra),
      %{schema: schema, changeset: changeset_fun, before: before_fun, after: after_fun}
    )
  end

  defp load_fun(schema, pks, config) do
    load_fun =
      case config[:load] do
        nil ->
          fn repo, change ->
            key = Enum.map(pks, fn col -> {col, Map.fetch!(change, to_string(col))} end)
            repo.get_by(schema, key)
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

      repo, _action, change ->
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

  @doc """
  Given a writer configuration created using `allow/3` translate the list of
  mutations into an `Ecto.Multi` operation.

  Example:

      %Ecto.Multi{} = mutation =
        Phoenix.Sync.Write.new()
        |> Phoenix.Sync.Write.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Write.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Write.apply(changes)

  If you want to add extra operations to the mutation transaction, beyond those
  applied by any `before` or `after` callbacks in your mutation config then use
  the functions in `Ecto.Multi` to do those as normal.

  Use `transaction/3` to apply the changes to the database and return the
  transaction id.
  """
  @spec apply(writer(), [mutation()]) :: Ecto.Multi.t()
  def apply(%__MODULE__{} = write, changes) when is_list(changes) do
    changes
    |> Enum.with_index()
    |> Enum.reduce(
      start_multi(),
      &apply_change(&2, &1, write)
    )
  end

  @txid_name {:__phoenix_sync__, :txid}

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
    with {:ok, %Mutation{operation: operation} = mutation} <- parse_change(change, write),
         {:ok, actions} <- mutation_actions(mutation, write),
         {:ok, action} <- Map.fetch(actions, operation),
         :ok <- validate_accept(operation, actions.accept) do
      multi
      |> mutation_changeset(mutation, n, action)
      |> validate_pks(mutation, n, action)
      |> apply_before(mutation, n, action)
      |> apply_changeset(mutation, n, action)
      |> apply_after(mutation, n, action)
    else
      {:error, reason} ->
        Ecto.Multi.error(multi, {:error, n, change}, reason)
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

  defp mutation_changeset(multi, %Mutation{} = mutation, n, action) do
    %{schema: schema, changeset: changeset_fun} = action
    %{operation: operation, data: lookup_data, changes: change_data} = mutation

    Ecto.Multi.run(multi, {:__phoenix_sync__, :changeset, n}, fn repo, _ ->
      case action.load.(repo, operation, lookup_data) do
        struct when is_struct(struct, schema) ->
          apply_changeset_fun(changeset_fun, struct, operation, change_data, action)

        struct when is_struct(struct) ->
          {:error,
           {"load function returned an inconsistent value. Expected %#{schema}{}, got %#{struct.__struct__}{}",
            mutation}}

        {:ok, struct} when is_struct(struct, schema) ->
          apply_changeset_fun(changeset_fun, struct, operation, change_data, action)

        {:ok, struct} when is_struct(struct) ->
          {:error,
           {"load function returned an inconsistent value. Expected %#{schema}{}, got %#{struct.__struct__}{}",
            mutation}}

        {:error, reason} = error ->
          {:error, {reason, mutation}}

        nil ->
          pks = Map.new(action.pks, fn col -> {col, Map.fetch!(lookup_data, to_string(col))} end)

          {:error, {"No original record found for row #{inspect(pks)}", mutation}}

        invalid ->
          {:error, {"Invalid return value from load(), got: #{inspect(invalid)}", mutation}}
      end
    end)
  end

  defp apply_changeset_fun(changeset_fun, data, op, change_data, action) do
    case changeset_fun do
      fun3 when is_function(fun3, 3) ->
        {:ok, fun3.(data, op, change_data)}

      fun2 when is_function(fun2, 2) ->
        {:ok, fun2.(data, change_data)}

      # delete changeset/validation functions can just look at the original
      fun1 when is_function(fun1, 1) ->
        {:ok, fun1.(data)}

      {m, f, a} ->
        l = length(a)

        cond do
          function_exported?(m, f, l + 3) ->
            {:ok, apply(m, f, [data, op, change_data | a])}

          function_exported?(m, f, l + 2) ->
            {:ok, apply(m, f, [data, change_data | a])}

          function_exported?(m, f, l + 1) ->
            {:ok, apply(m, f, [data | a])}

          true ->
            {:error,
             "Invalid changeset_fun for #{inspect(action.table)} #{inspect(op)}: #{inspect({m, f, a})}"}
        end

      _ ->
        {:error, "Invalid changeset_fun for #{inspect(action.table)} #{inspect(op)}"}
    end
  end

  defp validate_pks(multi, %Mutation{operation: :insert, changes: changes}, n, action) do
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
          {"Mutation data is missing required primary keys: #{inspect(keys)}", data}
        )
    end
  end

  defp apply_before(multi, mutation, n, %{before: before_fun} = action) do
    apply_hook(multi, mutation, n, before_fun, action)
  end

  defp apply_after(multi, mutation, n, %{after: after_fun} = action) do
    apply_hook(multi, mutation, n, after_fun, action)
  end

  defp apply_hook(multi, _mutation, _n, nil, _action) do
    multi
  end

  defp apply_hook(multi, mutation, n, hook_fun, action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = changeset!(changes, n)

      case hook_fun do
        # per-action callback
        fun3 when is_function(fun3, 3) ->
          fun3.(Ecto.Multi.new(), changeset, changes)

        # global callback including action
        fun4 when is_function(fun4, 4) ->
          fun4.(Ecto.Multi.new(), mutation.operation, changeset, changes)

        {m, f, a} ->
          l = length(a)

          cond do
            function_exported?(m, f, l + 3) ->
              apply(m, f, [Ecto.Multi.new(), changeset, changes | a])

            function_exported?(m, f, l + 4) ->
              apply(m, f, [Ecto.Multi.new(), mutation.operation, changeset, changes | a])

            true ->
              raise "Invalid after_fun for #{inspect(action.table)} #{inspect(mutation.operation)}: #{inspect({m, f, a})}"
          end

        _ ->
          raise "Invalid after_fun for #{inspect(action.table)} #{inspect(mutation.operation)}"
      end
      |> validate_callback!(mutation.operation, action)
    end)
  end

  defp changeset!(changes, n) do
    Map.fetch!(changes, {:__phoenix_sync__, :changeset, n})
  end

  defp apply_changeset(multi, %Mutation{operation: :insert}, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      Ecto.Multi.insert(Ecto.Multi.new(), {:__phoenix_sync__, :insert, n}, changeset!(changes, n))
    end)
  end

  defp apply_changeset(multi, %Mutation{operation: :update}, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      Ecto.Multi.update(Ecto.Multi.new(), {:__phoenix_sync__, :update, n}, changeset!(changes, n))
    end)
  end

  defp apply_changeset(multi, %Mutation{operation: :delete}, n, _action) do
    Ecto.Multi.merge(multi, fn changes ->
      Ecto.Multi.delete(Ecto.Multi.new(), {:__phoenix_sync__, :delete, n}, changeset!(changes, n))
    end)
  end

  defp validate_callback!(%Ecto.Multi{} = multi, _op, _action), do: multi

  defp validate_callback!(value, op, action),
    do:
      raise(ArgumentError,
        message:
          "Invalid return type #{inspect(value)} for #{op} into #{inspect(action.table)}. Expected %Ecto.Multi{}"
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

  defp validate_accept(op, allowed) do
    if op in allowed do
      :ok
    else
      {:error,
       "Action #{inspect(op)} not in :accept list: #{MapSet.to_list(allowed) |> inspect()}"}
    end
  end

  @doc """
  Runs the mutation inside a transaction.

  Since the mutation operation is expressed as an `Ecto.Multi` operation, see
  the [`Ecto.Repo`
  docs](https://hexdocs.pm/ecto/Ecto.Repo.html#c:transaction/2-use-with-ecto-multi)
  for the result if any of your mutations returns an error.

        Phoenix.Sync.Write.new()
        |> Phoenix.Sync.Write.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Write.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Write.apply(changes)
        |> Phoenix.Sync.Write.transaction(MyApp.Repo)
        |> case do
          {:ok, txid, _changes} ->
            # return the txid to the client
            Plug.Conn.send_resp(conn, 200, Jason.encode!(%{txid: txid}))
          {:error, _failed_operation, failed_value, _changes_so_far} ->
            # extract the error message from the changeset returned as `failed_value`
            error =
              Ecto.Changeset.traverse_errors(failed_value, fn {msg, opts} ->
                Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
                  opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
                end)
              end)
            Plug.Conn.send_resp(conn, 400, Jason.encode!(error))
          end
  """
  @spec transaction(Ecto.Multi.t(), Ecto.Repo.t(), keyword()) ::
          {:ok, integer(), any()} | Ecto.Multi.failure()
  def transaction(multi, repo, opts \\ [])

  def transaction(%Ecto.Multi{} = multi, repo, opts) when is_atom(repo) do
    with {:ok, changes} <- repo.transaction(multi, opts) do
      {txid, changes} = Map.pop!(changes, @txid_name)
      {:ok, txid, changes}
    end
  end

  @doc """
  Extract the transaction id from changes returned from `Repo.transaction`.

  This allows you to use a standard `c:Ecto.Repo.transaction/2` call to apply
  mutations defined using `apply/2` and extract the transaction id afterwards.

  Example

      {:ok, changes} =
        Phoenix.Sync.Write.new()
        |> Phoenix.Sync.Write.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Write.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Write.apply(changes)
        |> MyApp.Repo.transaction()

      {:ok, txid} = Phoenix.Sync.Write.txid(changes)
  """
  @spec txid(Ecto.Multi.changes()) :: {:ok, txid()} | :error
  def txid(%{@txid_name => txid} = _changes), do: {:ok, txid}
  def txid(_), do: :error

  @doc """
  Returns the transaction id from a `Ecto.Multi.changes()` result or raises if
  not found.

  See `txid/1`.
  """
  @spec txid!(Ecto.Multi.changes()) :: txid()
  def txid!(%{@txid_name => txid} = _changes), do: txid
  def txid!(_), do: raise(ArgumentError, message: "No txid in change data")
end
