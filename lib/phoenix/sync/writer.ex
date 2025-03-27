defmodule Phoenix.Sync.Writer do
  @moduledoc """
  Provides [optimistic write](https://electric-sql.com/docs/guides/writes)
  support for Phoenix- or Plug-based apps.

  Clients collect local transactional updates and then
  submit these transactions as a list of database operations to the Phoenix/Plug
  application.


      defmodule MutationController do
        use Phoenix.Controller, formats: [:json]

        alias #{inspect(__MODULE__)}

        def mutate(conn, %{"transaction" => transaction} = _params) do
          user_id = conn.assigns.user_id

          {:ok, txid, _changes} =
            Writer.new(format: Writer.Format.TanstackOptimistic)
            |> Writer.allow(
              Projects.Project,
              # authorizes writes against a Project just as your controller code would
              authorize: &Projects.authorize_project(&1, conn.assigns),
              # you can re-use your existing changeset/2 functions
              changeset: &Projects.Project.changeset/2
            )
            |> Writer.allow(
              Projects.Issue,
              authorize: &Projects.authorize_issue(&1, conn.assigns),
              # changeset defaults to Projects.Issue.changeset/2
            )
            |> Writer.apply(transaction, Repo)

          render(conn, :mutations, txid: txid)
        end
      end

  Local client writes are sent to the server as a list of mutations — a series
  of `INSERT`, `UPDATE` and `DELETE` operations and their associated data —
  with a single transaction represented by a single list of mutations.

  Using `#{inspect(__MODULE__)}` your application can then validate each of
  these mutation operations against its authentication and
  authorization logic and then apply them as a single transaction.

  The Postgres transaction id is returned to the client so that its optimistic
  update system can watch the Electric stream and match on the arrival of this
  transaction id.

  When the client receives this transaction id back through it's Electric sync
  stream then the client knows that it's up-to-date with the server.

  `#{inspect(__MODULE__)}` uses `Ecto.Multi`'s transaction update mechanism
  under the hood, which means that either all the operations in a client
  transaction are accepted or none are. See `apply/2` for how you can hook
  into the `Ecto.Multi` after applying your change data.

  > #### Warning {: .warning}
  >
  > The mutation operations received from clients should be considered as **untrusted**.
  >
  > Though the HTTP operation that uploaded them will have been authenticated and
  > authorized by your existing Plug middleware as usual, the actual content of the
  > request that is turned into writes against your database needs to be validated
  > very carefully against the privileges of the current user.

  ## Client Libraries

  `#{inspect(__MODULE__)}` does not require any particular client-side
  implementation, see Electric's [write pattern guides and example
  code](https://electric-sql.com/docs/guides/writes) for implementation
  strategies and examples.

  ### Existing libraries

  - [TanStack/optimistic](https://github.com/TanStack/optimistic) "A library
    for creating fast optimistic updates with flexible backend support that pairs
    seamlessly with sync engines"

    Integration:

        #{inspect(__MODULE__)}.new(format: #{inspect(__MODULE__.Format.TanstackOptimistic)})

  ## Server code

  Much as every controller action must be both authenticated and authorized to
  prevent users affecting data that they do not have permission to modify,
  mutations **MUST** be validated for both correctness — are the given values
  valid — and permissions: is the current user allowed to perform the given
  mutation?

  This dual verification, data and permissions, is performed by the combination
  of 5 application-defined callbacks for every model that you allow writes to:

  - [`load`](#module-load-function) - a function that takes the original data and returns the
    existing model from the database.
  - [`authorize`](#module-authorize-function) - a function that tests operations against the
    application's authentication/authorization logic.
  - [`changeset`](#module-changeset-function) - create and validate an `Ecto.Changeset` from the
    source data and mutation changes.
  - [`before` and `after`](#module-before-and-after-callbacks) - add arbitrary
    `Ecto.Multi` operations to the transaction based on the current operation.

  Calling `new/1` creates an empty writer configuration with the given mutation
  parser. But this alone does not permit any mutations. In order to allow
  writes from clients you must call `allow/3` with a schema module and some
  callback functions.

      # create an empty writer configuration which accepts writes in the given format
      writer = #{inspect(__MODULE__)}.new(format: #{inspect(__MODULE__.Format.TanstackOptimistic)})

      # allow writes to the `Todos.Todo` table
      # using `Todos.authorize_mutation/3` to validate mutation data before
      # touching the database
      writer = #{inspect(__MODULE__)}.allow(writer, Todos.Todo, authorize: &Todos.authorize_mutation/3)

  If the table name on the client differs from the Postgres table, then you add
  a `table` option that specifies the client table name that this `allow/3`
  call applies to:

      # `client_todos` is the name of the `todos` table on the clients
      writer =
        #{inspect(__MODULE__)}.allow(
          writer,
          Todos.Todo,
          validate: &Todos.validate_mutation/2,
          table: "client_todos"
        )

  ## Callbacks

  ### Load function

  The `load` callback takes the `data` in `update` or `delete` mutations, that is the original
  data before changes, and uses it to retreive the original `Ecto.Struct` model
  from the database.

  It can be a 1- or 2-arity function. The 1-arity version receives just the
  `data` parameters. The 2-arity version receives the `Ecto.Repo` that the
  transaction is being applied to and the `data` parameters.

      # 1-arity version
      def load(%{} = data) do
        # Repo.get(...)
      end

      # 2-arity version
      def load(repo, %{} = data) do
        # repo.get(...)
      end

  If not provided defaults to using `c:Ecto.Repo.get_by/3` using the primary
  key(s) defined on the model.

  For `insert` operations this load function is not used, the original struct
  is created by calling the `__struct__/0` function on the `Ecto.Schema`
  module.

  ### Authorize function

  The `authorize` option should be a 1-arity function whose purpose is to test
  the mutation data against the authorization rules for the application and
  model before attempting any database access.

  If the changes are valid then it should return `:ok` or `{:error, reason}` if
  they're invalid.

  If any of the changes fail the auth test, then the entire transaction will be
  rejected.

  This is the first line of defence against malicious writes as it provides a
  quick check of the data from the clients before any reads or writes to the
  database.

      def authorize(%#{inspect(__MODULE__.Operation)}{} = operation) do
        # :ok or {:error, "..."}
      end

  ### Changeset function

  The `changeset` callback performs the usual role of a changeset function: to
  validate the changes against the model's data constraints using the functions
  in `Ecto.Changeset`.

  It should return an `Ecto.Changeset` instance (or possibly the original
  schema struct in the case of `delete`s). If any of the transaction's
  changeset's are marked as invalid, then the entire transaction is aborted.

  If not specified, the `changeset` function is defaults to the model's
  standard `changeset/2` function if available.

  The callback can be either a 2- or 3-arity function.

  The 2-arity version will receive the `Ecto.Schema` struct returned from the
  `load` function and the mutation changes. The 3-arity version will receive
  the `load`ed struct, the changes and the operation.

      # 2-arity version
      def changeset(%Todo{} = data, %{} = changes) do
        data
        |> Ecto.Changeset.cast(changes, [:title, :completed])
        |> Ecto.Changeset.validate_required(changes, [:title])
      end

      # 3-arity version
      def changeset(%Todo{} = data, %{} = changes, operation)
          when operation in [:insert, :update, :delete] do
        # ...
      end

  #### Primary keys

  Whether the params for insert operations contains a value for the new primary
  key is application specific. It's certainly not required if you have declared your
  `Ecto.Schema` model with a primary key set to `autogenerate: true`.

  It's worth noting that if you are accepting primary key values as part of
  your `insert` changes, then you should use UUID primary keys for your models
  to prevent conflicts.

  ### Before and after callbacks

  These callbacks, run before or after the actual `insert`, `update` or
  `delete` operation allow for the addition of side effects to the transaction.

  They are passed an empty `Ecto.Multi` struct and which is then
  [merged](`Ecto.Multi.merge/2`) into the writer's transaction.

  They also allow for more validation/authorization steps as any operation
  within the callback that returns an "invalid" operation will abort the entire
  transaction.

      def before_or_after(%Ecto.Multi{} = multi, %Ecto.Changeset{} = change, %Writer.Context{} = context) do
        multi
        # add some side-effects
        # |> Ecto.Multi.run(Writer.operation_name(context, :image), fn _changes ->
        #   with :ok <- File.write(image.name, image.contents) do
        #    {:ok, nil}
        #   end
        # end)
        #
        # validate the current transaction and abort using an {:error, value} tuple
        # |> Ecto.Multi.run(Writer.operation_name(context, :my_validation), fn _changes ->
        #   {:error, "reject entire transaction"}
        # end)
      end

  Note the use of `operation_name/2` when adding operations. Every name in the
  final `Ecto.Multi` struct must be unique, `operation_name/2` generates names
  that are guaranteed to be unique to the current operation and callback.

  ### Per-operation callbacks

  If you want to re-use an existing function on a per-operation basis, then in
  your write configuration you can define both top-level and per operation
  callbacks:

      #{inspect(__MODULE__)}.allow(
        Todos.Todo,
        load: &Todos.fetch_for_user(&1, user_id),
        validate: &Todos.validate_mutation(&1, &2, user_id),
        changeset: &Todos.Todo.changeset/2,
        update: [
          # for inserts and deletes, use &Todos.Todo.changeset/2 but for updates
          # use this function
          changeset: &Todos.Todo.update_changeset/2,
          before: &Todos.before_update_todo/3
        ],
        insert: [
          # optional load, validate, changeset, before and after overrides for insert operations
        ],
        delete: [
          # optional load, validate, changeset, before and after overrides for delete operations
        ],
      )

  ## End-to-end usage

  The combination of the `load`, `validate` and `changeset` functions can be
  composed to provide strong guarantees of validity.

  The aim is to provide an interface as similar to that used in controller
  functions as possible.

  Here we show an example controller module that allows updating of `Todo`s via
  a standard `HTTP PUT` update handler and also via `HTTP POST`s to the
  `mutation` handler which applies optimistic writes via this module.

  We use the `load` function to validate the ownership of the original `Todo`
  by looking up the data using both the `id` and the `user_id`. This makes it
  impossible for user `a` to update todos belonging to user `b`.

      defmodule MyController do
        use Phoenix.Controller, formats: [:html, :json]

        # The standard HTTP PUT update handler
        def update(conn, %{"todo" => todo_params}) do
          user_id = conn.assigns.user_id

          with {:ok, todo} <- Todos.fetch_for_user(params, user_id),
               {:ok, params} <- validate_params(todo, todo_params, user_id),
               {:ok, updated_todo} <- Todos.update(todo, params) do
            redirect(conn, to: ~p"/todos/\#{updated_todo.id}")
          end
        end

        # The HTTP POST mutations handler which receives JSON data
        def mutations(conn, %{"transaction" => transaction} = _params) do
          user_id = conn.assigns.user_id

          {:ok, txid, _changes} =
            Write.new(format: Writer.Format.TanstackOptimistic)
            |> Write.allow(
              Todos.Todo,
              authorize: &validate_params(&1, user_id),
              load: &Todos.fetch_for_user(&1, user_id),
            )
            |> Write.apply(transaction, Repo)

          render(conn, :mutations, txid: txid)
        end

        defp validate_params(%{data: %{"user_id" => new_user_id}}, user_id)
            when new_user_id != user_id do
          {:error, "invalid user_id"}
        end

        defp validate_params(%{changes: %{"user_id" => new_user_id}}, user_id)
            when new_user_id != user_id do
          {:error, "invalid user_id"}
        end

        # also handles the case when there is no user_id in the params
        defp validate_params(%{} = params, _user_id) do
          :ok
        end
      end

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

  import Kernel, except: [apply: 2, apply: 3]

  defmodule Context do
    @moduledoc """
    Provides context within callback functions.

    ## Fields

      * `index` - the 0-indexed position of the current operation within the transaction
      * `changes` - the current `Ecto.Multi` changes so far
      * `operation` - the current `#{inspect(__MODULE__.Operation)}`
      * `callback` - the name of the current callback `:before` or `:after`
      * `schema` - the `Ecto.Schema` module associated with the current operation
    """

    @derive {Inspect, except: [:writer, :action]}

    defstruct [:index, :writer, :changes, :operation, :callback, :schema, :action, :pk]
  end

  defmodule Error do
    defexception [:message, :operation]

    @impl true
    def message(e) do
      "Operation #{inspect(e.operation)} failed: #{e.message}"
    end
  end

  require Record

  Record.defrecordp(:opkey, schema: nil, operation: nil, index: 0, pk: nil)

  alias __MODULE__.Operation
  alias __MODULE__.Transaction

  @operation_options [
    changeset: [
      type: {:fun, 2},
      doc: """
      A 2-arity function that returns a changeset for the given mutation data.
      """
    ],
    authorize: [
      type: {:fun, 1},
      doc: """
      A 1-arity function that validates the Write.Operation
      """
    ],
    before: [
      type: {:fun, 3},
      doc: """
      An optional callback that allows for the pre-pending of operations to the `Ecto.Multi`.
      """
    ],
    after: [
      type: {:fun, 3},
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
  @type txid() :: Transaction.id()
  @typedoc "Raw data from a client that will be parsed to a `#{inspect(__MODULE__.Transaction)}` by the Writer's format parser"
  @type transaction_data() :: term()
  @type context() :: %Context{
          index: non_neg_integer(),
          changes: Ecto.Mult.changes(),
          schema: Ecto.Schema.t(),
          operation: :insert | :update | :delete,
          callback: :load | :changeset | :before | :after
        }

  @type operation_opts() :: unquote([NimbleOptions.option_typespec(@operation_options_schema)])

  @operation_schema [
    type: :non_empty_keyword_list,
    keys: @operation_options,
    doc: NimbleOptions.docs(NimbleOptions.new!(@operation_options)),
    type_doc: "`t:operation_opts/0`"
  ]

  @writer_schema NimbleOptions.new!(
                   format: [
                     type: {:or, [:atom, {:fun, 1}, :mfa]},
                     required: true,
                     doc: """
                     A module implementing the `#{inspect(__MODULE__.Format)}`
                     behaviour or a function that takes mutation data from the client and
                     returns a `%#{inspect(__MODULE__.Transaction)}{}` struct.

                     See `#{inspect(__MODULE__.Format)}`
                     """,
                     type_spec: quote(do: (mutation() -> Operation.t()))
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
                  accept: [
                    type: {:list, {:in, @operations}},
                    doc: """
                    A list of actions to accept. Defaults to accepting all operations, `#{inspect(@operations)}`.

                    A transaction containing an operation not in the accept list will be rejected.
                    """
                  ],
                  load: [
                    type: {:or, [{:fun, 1}, {:fun, 2}]},
                    doc: """
                    A 1- or 2-arity function that accepts either the
                    mutation data or an `Ecto.Repo` instance and the mutation data and returns
                    the original row from the database.

                    Valid return values are:

                    - `struct()` - an `Ecto.Schema` struct, that must match the
                      module passed to `allow/3`
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
                  authorize: [
                    type: {:fun, 1},
                    doc: """
                    A function that checks the pending %#{inspect(__MODULE__.Operation)}{}
                    against your application's authorization logic.

                    This is run before any database access is performed and so provides an
                    efficient way to prevent malicious writes without loading your database.
                    """,
                    type_spec: quote(do: (Operation.t() -> :ok | {:error, term()}))
                  ],
                  changeset: [
                    type: {:or, [{:fun, 2}, {:fun, 3}]},
                    doc: """
                    a 2- or 3-arity function that returns
                    an `Ecto.Changeset` for a given mutation.

                    ### Callback params

                    - `data` an Ecto.Schema struct matching the one used when
                      calling `allow/2` returned from the `load` function.
                    - `operation` the operation action, one of `:insert`, `:update` or `:delete`
                    - `changes` a map of changes to apply to the `data`.

                    At absolute minimum, this should call
                    `Ecto.Changeset.cast/3` to validate the proposed data:

                        def my_changeset(data, _operation, changes) do
                          Ecto.Changeset.cast(data, changes, @permitted_columns)
                        end
                    """,
                    type_spec:
                      quote(do: (Ecto.Schema.t(), operation(), data() -> Ecto.Changeset.t()))
                  ],
                  before: [
                    type: {:fun, 3},
                    doc: """
                    an optional callback that allows for the pre-pending of
                    operations to the `Ecto.Multi` representing a mutation transaction.

                    If should be a 4-arity function.

                    ### Arguments

                    - `multi` - an empty `%Ecto.Multi{}` instance that you should apply
                      your actions to
                    - `changeset` - the changeset representing the individual mutation operation
                    - `context` - the current change [context](`#{__MODULE__.Context}`)

                    The result should be the `Ecto.Multi` instance which will be
                    [merged](`Ecto.Multi.merge/2`) with the one representing the mutation
                    operation.

                    Because every action in an `Ecto.Multi` must have a unique
                    key, we advise using the `operation_name/2` function to generate a unique
                    operation name based on the `context`.

                        def my_before(multi, :insert, changeset, context) do
                          name = Writer.operation_name(context, :event_insert)
                          Ecto.Multi.insert(multi, name, %Event{todo_id: id})
                        end
                    """,
                    type_spec:
                      quote(do: (Ecto.Multi.t(), Ecto.Changeset.t(), context() -> Ecto.Multi.t()))
                  ],
                  after: [
                    type: {:fun, 3},
                    doc: """
                    an optional callback function that allows for the
                    appending of operations to the `Ecto.Multi` representing a mutation
                    transaction.

                    See the docs for `:before` for the function signature and arguments.
                    """,
                    type_spec:
                      quote(do: (Ecto.Multi.t(), Ecto.Changeset.t(), context() -> Ecto.Multi.t()))
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
                      &Keyword.put(&1, :changeset, type: {:or, [{:fun, 1}, {:fun, 2}]})
                    )
                    |> Keyword.put(:doc, """
                    Callbacks for validating and modifying `delete` operations.
                    See the documentation for `insert`.
                    """)
                )

  defstruct format: nil, mappings: %{}

  @type writer_opts() :: [unquote(NimbleOptions.option_typespec(@writer_schema))]
  @type allow_opts() :: [unquote(NimbleOptions.option_typespec(@allow_schema))]

  @type t() :: %__MODULE__{}
  @type writer() :: t()

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
    format = Keyword.fetch!(config, :format)
    %__MODULE__{format: format}
  end

  @doc """
  Allow writes to the given `Ecto.Schema`.

  Only tables specified in calls to `allow/3` will be accepted by later calls
  to `apply/2`. Any changes to tables not explicitly defined by `allow/3` calls
  will be rejected and cause the entire transaction to be rejected.

  ## Examples

      # allow writes to the Todo table using
      # `MyApp.Todos.Todo.authorize_mutation/1` to validate operations
      Phoenix.Sync.Writer.new(format: MyApp.OperationFormat)
      |> Phoenix.Sync.Writer.allow(
        MyApp.Todos.Todo,
        authorize: &MyApp.Todos.authorize_mutation/1
      )

      # A more complex configuration adding an `after` callback to inserts
      # and using a custom query to load the original database value.
      Phoenix.Sync.Writer.new(format: MyApp.OperationFormat)
      |> Phoenix.Sync.Writer.allow(
        MyApp.Todos..Todo,
        load: &MyApp.Todos.get_for_mutation/1,
        authorize: &MyApp.Todos.authorize_mutation/1,
        insert: [
          after: &MyApp.Todos.after_insert_mutation/3
        ]
      )

  ## Supported options

  #{NimbleOptions.docs(@allow_schema)}

  """
  @spec allow(writer(), module(), allow_opts()) :: writer()
  def allow(writer, schema, opts \\ [])

  def allow(%__MODULE__{} = write, schema, opts) when is_atom(schema) do
    {schema, table, pks} = validate_schema!(schema)

    config = NimbleOptions.validate!(opts, @allow_schema)

    key = config[:table] || table
    load_fun = load_fun(schema, pks, opts)
    authorize_fun = authorize_fun(opts)

    accept = Keyword.get(config, :accept, @operations) |> MapSet.new()

    table_config = %{
      schema: schema,
      table: table,
      pks: pks,
      accept: accept,
      authorize: authorize_fun
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
            key = Map.new(pks, fn col -> {col, Map.fetch!(change, to_string(col))} end)
            repo.get_by(schema, key)
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
      _repo, :insert, _change, _ctx ->
        schema.__struct__()

      repo, _action, change, ctx ->
        # look for the data in the changes so far. uses the multi changes from
        # append_changeset_result/3
        pk =
          Map.new(pks, fn col ->
            {to_string(col), change |> Map.fetch!(to_string(col)) |> to_string()}
          end)

        Enum.filter(ctx.changes, fn
          {opkey(schema: ^schema, pk: ^pk), _value} = change -> change
          _ -> nil
        end)
        |> Enum.max_by(fn {opkey(index: index), _} -> index end, &>=/2, fn -> nil end)
        |> case do
          nil ->
            # not in changeset, so call load
            load_fun.(repo, change)

          {opkey(operation: :delete), _} ->
            # last op on this key was a delete
            nil

          {_opkey, value} ->
            {:ok, value}
        end
    end
  end

  defp authorize_fun(opts) do
    case opts[:authorize] do
      nil ->
        fn _ -> :ok end

      fun1 when is_function(fun1, 1) ->
        fun1

      fun when is_function(fun) ->
        info = Function.info(fun)

        raise ArgumentError,
          message:
            "Invalid authorize function. Expected a 1-arity function but got arity #{info[:arity]}"

      invalid ->
        raise ArgumentError,
          message:
            "Invalid authorize function. Expected a 1-arity function but #{inspect(invalid)}"
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
  Apply and write changes to the given repo in a single call.

      writer
      |> #{inspect(__MODULE__)}.apply(changes, Repo)

  is equivalent to:

      writer
      |> #{inspect(__MODULE__)}.apply(changes)
      |> #{inspect(__MODULE__)}.transaction(Repo)
  """
  @spec apply(writer(), transaction_data(), Ecto.Repo.t()) ::
          {:ok, txid(), Ecto.Multi.changes()} | Ecto.Multi.failure()
  def apply(%__MODULE__{} = write, changes, repo) when is_atom(repo) and is_list(changes) do
    write
    |> apply(changes)
    |> transaction(repo)
  end

  @doc """
  Given a writer configuration created using `allow/3` translate the list of
  mutations into an `Ecto.Multi` operation.

  Example:

      %Ecto.Multi{} = multi =
        #{inspect(__MODULE__)}.new(format: #{inspect(__MODULE__.Format.TanstackOptimistic)})
        |> #{inspect(__MODULE__)}.allow(MyApp.Todos.Todo, authorize: &my_authz_function/1)
        |> #{inspect(__MODULE__)}.allow(MyApp.Options.Option, authorize: &my_authz_function/1)
        |> #{inspect(__MODULE__)}.apply(changes)

  If you want to add extra operations to the mutation transaction, beyond those
  applied by any `before` or `after` callbacks in your mutation config then use
  the functions in `Ecto.Multi` to do those as normal.

  Use `transaction/3` to apply the changes to the database and return the
  transaction id.
  """
  @spec apply(writer(), transaction_data()) :: Ecto.Multi.t()
  def apply(%__MODULE__{} = writer, changes) do
    # Want to return a multi here but have that multi fail without contacting
    # the db if any of the authorize calls fail.
    #
    # Looking at `Ecto.Multi.__apply__/4` it first checks for invalid
    # operations before doing anything. So i can just add an error to a blank
    # multi and return that and the transaction step will fail before touching
    # the repo.
    with {:parse, {:ok, %Transaction{} = txn}} <- {:parse, parse_transaction(writer, changes)},
         {:authorize, :ok} <- {:authorize, authorize_transaction(writer, txn)} do
      txn.operations
      |> Enum.reduce(
        start_multi(txn),
        &apply_change(&2, &1, writer)
      )
    else
      {step, {:error, error}} ->
        Ecto.Multi.error(Ecto.Multi.new(), step, error)
    end
  end

  defp parse_transaction(%__MODULE__{} = writer, changes) do
    case writer.format do
      module when is_atom(module) ->
        if Code.ensure_loaded?(module) && function_exported?(module, :parse_transaction, 1) do
          module.parse_transaction(changes)
        else
          {:error,
           "#{inspect(module)} does not implement the #{inspect(__MODULE__.Format)} behaviour"}
        end

      fun when is_function(fun, 1) ->
        fun.(changes)

      {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) ->
        Kernel.apply(m, f, [changes | a])
    end
  end

  defp authorize_transaction(%__MODULE__{} = writer, %Transaction{} = txn) do
    Enum.reduce_while(txn.operations, :ok, fn operation, :ok ->
      case mutation_actions(operation, writer) do
        {:ok, actions} ->
          %{authorize: authorize, accept: accept} = actions

          if MapSet.member?(accept, operation.operation) do
            case authorize.(operation) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, %Error{message: reason, operation: operation}}}
            end
          else
            {:halt,
             {:error,
              %Error{
                message:
                  "Action #{inspect(operation.operation)} not in :accept list: #{MapSet.to_list(accept) |> inspect()}",
                operation: operation
              }}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  @txn_name {:__phoenix_sync__, :txn}
  @txid_name {:__phoenix_sync__, :txid}

  defp start_multi(txn) do
    Ecto.Multi.new()
    |> Ecto.Multi.put(@txn_name, txn)
    |> Ecto.Multi.run(@txid_name, fn repo, _ ->
      with {:ok, %{rows: [[txid]]}} <- repo.query("SELECT txid_current() as txid") do
        {:ok, txid}
      end
    end)
  end

  defp apply_change(multi, %Operation{} = op, %__MODULE__{} = writer) do
    with {:ok, actions} <- mutation_actions(op, writer),
         {:ok, action} <- Map.fetch(actions, op.operation) do
      ctx = %Context{
        index: op.index,
        writer: writer,
        operation: op,
        schema: action.schema,
        action: action
      }

      multi
      |> mutation_changeset(op, ctx, action)
      |> validate_pks(op, ctx, action)
      |> apply_before(op, ctx, action)
      |> apply_changeset(op, ctx, action)
      |> apply_after(op, ctx, action)
    else
      {:error, reason} ->
        Ecto.Multi.error(multi, {:error, op.index}, %Error{message: reason, operation: op})
    end
  end

  defp mutation_changeset(multi, %Operation{} = op, %Context{} = ctx, action) do
    %{schema: schema, changeset: changeset_fun} = action
    %{index: idx, operation: operation, data: lookup_data, changes: change_data} = op

    Ecto.Multi.run(multi, {:__phoenix_sync__, :changeset, idx}, fn repo, changes ->
      ctx = %{ctx | changes: changes}

      case action.load.(repo, operation, lookup_data, ctx) do
        struct when is_struct(struct, schema) ->
          apply_changeset_fun(changeset_fun, struct, op, change_data, action)

        struct when is_struct(struct) ->
          {:error,
           %Error{
             message:
               "load function returned an inconsistent value. Expected %#{schema}{}, got %#{struct.__struct__}{}",
             operation: op
           }}

        {:ok, struct} when is_struct(struct, schema) ->
          apply_changeset_fun(changeset_fun, struct, op, change_data, action)

        {:ok, struct} when is_struct(struct) ->
          {:error,
           %Error{
             message:
               "load function returned an inconsistent value. Expected %#{schema}{}, got %#{struct.__struct__}{}",
             operation: op
           }}

        {:error, reason} ->
          {:error, %Error{message: reason, operation: op}}

        nil ->
          pks = Map.new(action.pks, fn col -> {col, Map.fetch!(lookup_data, to_string(col))} end)

          {:error,
           %Error{message: "No original record found for row #{inspect(pks)}", operation: op}}

        invalid ->
          {:error,
           %Error{
             message: "Invalid return value from load(), got: #{inspect(invalid)}",
             operation: op
           }}
      end
    end)
  end

  defp apply_changeset_fun(changeset_fun, data, op, change_data, action) do
    case changeset_fun do
      fun3 when is_function(fun3, 3) ->
        {:ok, fun3.(data, op.operation, change_data)}

      fun2 when is_function(fun2, 2) ->
        {:ok, fun2.(data, change_data)}

      # delete changeset/validation functions can just look at the original
      fun1 when is_function(fun1, 1) ->
        {:ok, fun1.(data)}

      _ ->
        {:error, "Invalid changeset_fun for #{inspect(action.table)} #{inspect(op)}"}
    end
  end

  # inserts don't need the pk fields
  defp validate_pks(multi, %Operation{operation: :insert}, _ctx, _action) do
    multi
  end

  defp validate_pks(multi, %Operation{index: idx, data: lookup}, _ctx, action) do
    do_validate_pks(multi, action.pks, lookup, idx)
  end

  defp do_validate_pks(multi, pks, data, n) do
    case Enum.reject(pks, &Map.has_key?(data, to_string(&1))) do
      [] ->
        multi

      keys ->
        Ecto.Multi.error(
          multi,
          {:error, n},
          {"Operation data is missing required primary keys: #{inspect(keys)}", data}
        )
    end
  end

  defp apply_before(multi, operation, ctx, %{before: before_fun} = action) do
    apply_hook(multi, operation, ctx, {:before, before_fun}, action)
  end

  defp apply_after(multi, operation, ctx, %{after: after_fun} = action) do
    apply_hook(multi, operation, ctx, {:after, after_fun}, action)
  end

  defp apply_hook(multi, _operation, _ctx, {_, nil}, _action) do
    multi
  end

  defp apply_hook(multi, operation, ctx, {hook_name, hook_fun}, action) do
    Ecto.Multi.merge(multi, fn changes ->
      changeset = changeset!(changes, operation)

      ctx = %{ctx | changes: changes, callback: hook_name}

      case hook_fun do
        fun3 when is_function(fun3, 3) ->
          fun3.(Ecto.Multi.new(), changeset, ctx)

        _ ->
          raise "Invalid after_fun for #{inspect(action.table)} #{inspect(operation.operation)}"
      end
      |> validate_callback!(operation.operation, action)
    end)
  end

  defp validate_callback!(%Ecto.Multi{} = multi, _op, _action), do: multi

  defp validate_callback!(value, op, action),
    do:
      raise(ArgumentError,
        message:
          "Invalid return type #{inspect(value)} for #{op} into #{inspect(action.table)}. Expected %Ecto.Multi{}"
      )

  defp changeset!(changes, %Operation{index: n}) do
    changeset!(changes, n)
  end

  defp changeset!(changes, n) when is_integer(n) do
    Map.fetch!(changes, {:__phoenix_sync__, :changeset, n})
  end

  defp apply_changeset(multi, %Operation{operation: :insert} = op, ctx, action) do
    Ecto.Multi.merge(multi, fn changes ->
      Ecto.Multi.insert(Ecto.Multi.new(), operation_name(ctx), changeset!(changes, op))
    end)
    |> append_changeset_result(ctx, action)
  end

  defp apply_changeset(multi, %Operation{operation: :update} = op, ctx, action) do
    Ecto.Multi.merge(multi, fn changes ->
      Ecto.Multi.update(Ecto.Multi.new(), operation_name(ctx), changeset!(changes, op))
    end)
    |> append_changeset_result(ctx, action)
  end

  defp apply_changeset(multi, %Operation{operation: :delete} = op, ctx, action) do
    Ecto.Multi.merge(multi, fn changes ->
      Ecto.Multi.delete(Ecto.Multi.new(), operation_name(ctx), changeset!(changes, op))
    end)
    |> append_changeset_result(ctx, action)
  end

  # Put the result of each op into the multi under a deterministic key based on
  # the primary key.
  # This allows for quick lookup of data that we've already accessed
  defp append_changeset_result(multi, ctx, action) do
    name = operation_name(ctx)

    Ecto.Multi.merge(multi, fn %{^name => result} ->
      pk = Map.new(action.pks, &{to_string(&1), Map.fetch!(result, &1) |> to_string()})

      Ecto.Multi.put(Ecto.Multi.new(), load_key(ctx, pk), result)
    end)
  end

  defp mutation_actions(%Operation{relation: [prefix, name] = relation} = operation, write)
       when is_binary(name) and is_binary(prefix) do
    case write.mappings do
      %{^relation => actions} ->
        {:ok, actions}

      %{^name => actions} ->
        {:ok, actions}

      _ ->
        {:error,
         %Error{
           message: "No configuration for writes to table #{inspect(name)}",
           operation: operation
         }}
    end
  end

  defp mutation_actions(%Operation{relation: name} = operation, write) when is_binary(name) do
    case write.mappings do
      %{^name => actions} ->
        {:ok, actions}

      mappings ->
        case Enum.filter(Map.keys(mappings), &match?([_, ^name], &1)) do
          [] ->
            {:error,
             %Error{
               message: "No configuration for writes to table #{inspect(name)}",
               operation: operation
             }}

          [key] ->
            {:ok, Map.fetch!(write.mappings, key)}

          [_ | _] = keys ->
            {:error,
             %Error{
               message:
                 "Multiple matches for relation #{inspect(name)}: #{inspect(keys)}. Please pass full `[\"schema\", \"name\"]` relation in mutation data",
               operation: operation
             }}
        end
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
          {:ok, txid(), Ecto.Multi.changes()} | Ecto.Multi.failure()
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

  def operation_name(%Context{} = ctx) do
    {ctx.schema, ctx.operation.operation, ctx.index}
  end

  @doc """
  ## TODO
  """
  @spec operation_name(context(), term()) :: term()
  def operation_name(%Context{} = ctx, label) do
    {operation_name(ctx), label}
  end

  @doc """
  ## TODO
  """
  @spec fetch_or_load(context(), map()) :: {:ok, Ecto.Schema.t()} | {:error, term()}
  def fetch_or_load(%Context{} = _ctx, _attrs) do
    raise "implement"
  end

  defp load_key(ctx, pk) do
    opkey(schema: ctx.schema, operation: ctx.operation.operation, index: ctx.index, pk: pk)
  end
end
