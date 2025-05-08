defmodule Phoenix.Sync.Writer do
  @moduledoc """
  Provides [write-path sync](https://electric-sql.com/docs/guides/writes) support for
  Phoenix- or Plug-based apps.

  Imagine you're building an application on sync. You've used the
  [read-path sync utilities](https://hexdocs.pm/phoenix_sync/readme.md#read-path-sync)
  to sync data into the front-end. If the client then changes the data locally, these
  writes can be batched up and sent back to the server.

  `#{inspect(__MODULE__)}` provides a principled way of ingesting these local writes
  and applying them to Postgres. In a way that works-with and re-uses your existing
  authorization logic and your existing `Ecto.Schema`s and `Ecto.Changeset` validation
  functions.

  This allows you to build instant, offline-capable applications that work with
  [local optimistic state](https://electric-sql.com/docs/guides/writes).

  For example, take a project management app that's using
  [@TanStack/optimistic](https://github.com/TanStack/optimistic) to batch up local
  optimistic writes and POST them to the `Phoenix.Controller` below:

      defmodule MutationController do
        use Phoenix.Controller, formats: [:json]

        alias Phoenix.Sync.Writer
        alias Phoenix.Sync.Writer.Format

        def mutate(conn, %{"transaction" => transaction} = _params) do
          user_id = conn.assigns.user_id

          {:ok, txid, _changes} =
            Phoenix.Sync.Writer.new()
            |> Phoenix.Sync.Writer.allow(
              Projects.Project,
              check: reject_invalid_params/2,
              load: &Projects.load_for_user(&1, user_id),
              validate: &Projects.Project.changeset/2
            )
            |> Phoenix.Sync.Writer.allow(
              Projects.Issue,
              # Use the sensible defaults:
              # load: Ecto.Repo.get_by(Projects.Issue, id: ^issue_id)
              # validate: Projects.Issue.changeset/2
              # etc.
            )
            |> Phoenix.Sync.Writer.apply(transaction, Repo, format: Format.TanstackOptimistic)

          render(conn, :mutations, txid: txid)
        end
      end

  The controller constructs a `#{inspect(__MODULE__)}` instance and pipes it
  through a series of `Writer.allow/3` calls, registering functions against
  `Ecto.Schema`s (in this case `Projects.Project` and `Projects.Issue`) to
  validate and authorize each of these mutation operations before applying
  them as a single transaction.

  Local client writes are sent to the server as a list of mutations — a series
  of `INSERT`, `UPDATE` and `DELETE` operations and their associated data —
  with a single transaction represented by a single list of mutations.

  > #### Warning {: .warning}
  >
  > The mutation operations received from clients MUST be considered as **untrusted**.
  >
  > Though the HTTP operation that uploaded them will have been authenticated and
  > authorized by your existing Plug middleware as usual, the actual content of the
  > request that is turned into writes against your database needs to be validated
  > very carefully against the privileges of the current user.
  >
  > That's what `#{inspect(__MODULE__)}` is for: specifying which resources can be
  > updated and registering functions to authorize and validate the mutation payload.

  ## Transactions

  The `{:ok, txid, changes}` return value from `Phoenix.Sync.Writer.apply/4`
  allows the Postgres transaction ID to be returned to the client in the response data.

  This allows clients to monitor the read-path sync stream and match on the
  arrival of the same transaction id. When the client receives this transaction id
  back through its sync stream, it knows that it can discard the local optimistic
  state for that transaction. (This is a more robust way of managing optimistic state
  than just matching on instance IDs, as it allows for local changes to be rebased
  on concurrent changes to the same date from other users).

  `#{inspect(__MODULE__)}` uses `Ecto.Multi`'s transaction update mechanism
  under the hood, which means that either all the operations in a client
  transaction are accepted or none are. See `to_multi/1` for how you can hook
  into the `Ecto.Multi` after applying your change data.

  > #### Compatibility {: .info}
  >
  > `#{inspect(__MODULE__)}` can only return transaction ids when connecting to
  > a Postgres database (a repo with `adapter: Ecto.Adapters.Postgres`). You can
  > use this module for other databases, but the returned txid will be `nil`.

  ## Client Libraries

  `#{inspect(__MODULE__)}` is not coupled to any particular client-side implementation.
  See Electric's [write pattern guides and example code](https://electric-sql.com/docs/guides/writes)
  for implementation strategies and examples.

  Instead, `#{inspect(__MODULE__)}` provides an adapter pattern where you can register
  a `format` adapter or `parser` function to parse the expected payload format from a client side library
  into the struct that `#{inspect(__MODULE__)}` expects.

  The currently supported format adapters are:

  - [TanStack/optimistic](https://github.com/TanStack/optimistic) "A library
    for creating fast optimistic updates with flexible backend support that pairs
    seamlessly with sync engines"

    Integration:

        #{inspect(__MODULE__)}.new()
        |> #{inspect(__MODULE__)}.ingest(mutation_data, format: #{inspect(__MODULE__.Format.TanstackOptimistic)})
        |> #{inspect(__MODULE__)}.transaction(Repo)

  ## Usage

  Much as every controller action must be authenticated, authorized and validated
  to prevent users writing invalid data or data that they do not have permission
  to modify, mutations **MUST** be validated for both correctness (are the given
  values valid?) and permissions (is the current user allowed to apply the given
  mutation?).

  This dual verification -- of data and permissions -- is performed by a pipeline
  of application-defined callbacks for every model that you allow writes to:

  - [`check`](#module-check-function) - a function that performs a "pre-flight"
    sanity check of the user-provided data in the mutation; this should just
    validate the data and not usually hit the database; checks are performed on
    all operations in a transaction before proceeding to the next steps in the
    pipeline; this allows for fast rejection of invalid data before performing
    more expensive operations
  - [`load`](#module-load-function) - a function that takes the original data
    and returns the existing model from the database, if it exists, for an update
    or delete operation
  - [`validate`](#module-validate-function) - create and validate an `Ecto.Changeset`
    from the source data and mutation changes; this is intended to be compatible with
    using existing schema changeset functions; note that, as per any changeset function,
    the validate function can perform both authorization and validation
  - [`pre_apply` and `post_apply`](#module-pre_apply-and-post_apply-callbacks) - add
    arbitrary `Ecto.Multi` operations to the transaction based on the current operation

  See `apply/2` for how the transaction is processed internally and how best to
  use these callback functions to express your app's authorization and validation
  requirements.

  Calling `new/0` creates an empty writer configuration with the given mutation
  parser. But this alone does not permit any mutations. In order to allow writes
  from clients you must call `allow/3` with a schema module and some callback functions.

      # create an empty writer configuration
      writer = #{inspect(__MODULE__)}.new()

      # allow writes to the `Todos.Todo` table
      # using `Todos.check_mutation/1` to validate mutation data before
      # touching the database
      writer = #{inspect(__MODULE__)}.allow(writer, Todos.Todo, check: &Todos.check_mutation/1)

  If the table name on the client differs from the Postgres table, then you can
  add a `table` option that specifies the client table name that this `allow/3`
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

  ### Check

  The `check` option should be a 1-arity function whose purpose is to test
  the mutation data against the authorization rules for the application and
  model before attempting any database access.

  If the changes are valid then it should return `:ok` or `{:error, reason}` if
  they're invalid.

  If any of the changes fail the auth test, then the entire transaction will be
  rejected.

  This is the first line of defence against malicious writes as it provides a
  quick check of the data from the clients before any reads or writes to the
  database.

  Note that the writer pipeline checks all the operations before proceeding to
  load, validate and apply each operation in turn.

      def check(%#{inspect(__MODULE__.Operation)}{} = operation) do
        # :ok or {:error, "..."}
      end

  ### Load

  The `load` callback takes the `data` in `update` or `delete` mutations (i.e.:
  the original data before changes), and uses it to retrieve the original
  `Ecto.Struct` model from the database.

  It can be a 1- or 2-arity function. The 1-arity version receives just the
  `data` parameters. The 2-arity version receives the `Ecto.Repo` that the
  transaction is being applied to and the `data` parameters.

      # 1-arity version
      def load(%{"column" => "value"} = data) do
        # Repo.get(...)
      end

      # 2-arity version
      def load(repo, %{"column" => "value"} = data) do
        # repo.get(...)
      end

  If not provided defaults to using `c:Ecto.Repo.get_by/3` using the primary
  key(s) defined on the model.

  For `insert` operations this load function is not used. Instead, the original
  struct is created by calling the `__struct__/0` function on the `Ecto.Schema`
  module.

  ### Validate

  The `validate` callback performs the usual role of a changeset function: to
  validate the changes against the model's data constraints using the functions
  in `Ecto.Changeset`.

  It should return an `Ecto.Changeset` instance (or possibly the original
  schema struct in the case of `delete`s). If any of the transaction's
  changeset's are marked as invalid, then the entire transaction is aborted.

  If not specified, the `validate` function is defaulted to the schema model's
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

  ### pre_apply and post_apply

  These callbacks, run before or after the actual `insert`, `update` or
  `delete` operation allow for the addition of side effects to the transaction.

  They are passed an empty `Ecto.Multi` struct and which is then
  [merged](`Ecto.Multi.merge/2`) into the writer's transaction.

  They also allow for more validation/authorization steps as any operation
  within the callback that returns an "invalid" operation will abort the entire
  transaction.

      def pre_or_post_apply(%Ecto.Multi{} = multi, %Ecto.Changeset{} = change, %Phoenix.Sync.Writer.Context{} = context) do
        multi
        # add some side-effects
        # |> Ecto.Multi.run(Phoenix.Sync.Writer.operation_name(context, :image), fn _changes ->
        #   with :ok <- File.write(image.name, image.contents) do
        #    {:ok, nil}
        #   end
        # end)
        #
        # validate the current transaction and abort using an {:error, value} tuple
        # |> Ecto.Multi.run(Phoenix.Sync.Writer.operation_name(context, :my_validation), fn _changes ->
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
        check: &Todos.check_mutation(&1, &2, user_id),
        validate: &Todos.Todo.changeset/2,
        update: [
          # for inserts and deletes, use &Todos.Todo.changeset/2 but for updates
          # use this function
          validate: &Todos.Todo.update_changeset/2,
          pre_apply: &Todos.pre_apply_update_todo/3
        ],
        insert: [
          # optional validate, pre_apply and post_apply
          # overrides for insert operations
        ],
        delete: [
          # optional validate, pre_apply and post_apply
          # overrides for delete operations
        ],
      )

  ## End-to-end usage

  The combination of the `check`, `load`, `validate`, `pre_apply` and
  `post_apply` functions can be composed to provide strong guarantees of
  validity.

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

        alias Phoenix.Sync.Writer

        # The standard HTTP PUT update handler
        def update(conn, %{"todo" => todo_params}) do
          user_id = conn.assigns.user_id

          with {:ok, todo} <- fetch_for_user(params, user_id),
               {:ok, params} <- validate_params(todo, todo_params, user_id),
               {:ok, updated_todo} <- Todos.update(todo, params) do
            redirect(conn, to: ~p"/todos/\#{updated_todo.id}")
          end
        end

        # The HTTP POST mutations handler which receives JSON data
        def mutations(conn, %{"transaction" => transaction} = _params) do
          user_id = conn.assigns.user_id

          {:ok, txid, _changes} =
            Writer.new()
            |> Writer.allow(
              Todos.Todo,
              check: &validate_mutation(&1, user_id),
              load: &fetch_for_user(&1, user_id),
            )
            |> Writer.apply(transaction, Repo, format: Writer.Format.TanstackOptimistic)

          render(conn, :mutations, txid: txid)
        end

        # Included here for completeness but in a real app would be a
        # public function in the Todos context.
        # Because we're validating the ownership of the Todo here we add an
        # extra layer of auth checks, preventing one user from modifying
        # the Todos of another.
        defp fetch_for_user(%{"id" => id}, user_id) do
          from(t in Todos.Todo, where: t.id == ^id  and t.user_id == ^user_id)
          |> Repo.one()
        end

        defp validate_mutation(%Writer.Operation{} = op, user_id) do
          with :ok <- validate_params(op.data, user_id) do
            validate_params(op.changes, user_id)
          end
        end

        defp validate_params(%{"user_id" => user_id}, user_id), do: :ok
        defp validate_params(%{} = _params, _user_id), do: {:error, "invalid user_id"}
      end

  Because `Phoenix.Sync.Write` leverages `Ecto.Multi` to do the work of
  applying changes and managing errors, you're also free to extend the actions
  that are performed with every transaction using `pre_apply` and `post_apply`
  callbacks configured per-table or per-table per-action (insert, update,
  delete). See `allow/3` for more information on the configuration options
  for each table.

  The result of `to_multi/1` or `to_multi/3` is an `Ecto.Multi` instance so you can also just
  append operations using the normal `Ecto.Multi` functions:

      {:ok, txid, _changes} =
        Writer.new()
        |> Writer.allow(Todo, ...)
        |> Writer.to_multi(transaction, parser: &my_transaction_parser/1)
        |> Ecto.Multi.insert(:my_action, %Event{})
        |> Writer.transaction(Repo)
  """

  import Kernel, except: [apply: 2, apply: 3]

  defmodule Context do
    @moduledoc """
    Provides context within callback functions.

    ## Fields

      * `index` - the 0-indexed position of the current operation within the transaction
      * `changes` - the current `Ecto.Multi` changes so far
      * `operation` - the current `#{inspect(__MODULE__.Operation)}`
      * `callback` - the name of the current callback, `:pre_apply` or `:post_apply`
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
  require Logger

  Record.defrecordp(:opkey, schema: nil, operation: nil, index: 0, pk: nil)

  alias __MODULE__.Operation
  alias __MODULE__.Transaction
  alias __MODULE__.Format

  @type pre_post_func() :: (Ecto.Multi.t(), Ecto.Changeset.t(), context() -> Ecto.Multi.t())

  @operation_options [
    validate: [
      type: {:fun, 2},
      doc: """
      A 2-arity function that returns a changeset for the given mutation data.

      Arguments:

      - `schema` the original `Ecto.Schema` model returned from the `load` function
      - `changes` a map of changes from the mutation operation

      Return value:

      - an `Ecto.Changeset`
      """,
      type_spec: quote(do: (Ecto.Schema.t(), data() -> Ecto.Changeset.t()))
    ],
    pre_apply: [
      type: {:fun, 3},
      doc: """
      An optional callback that allows for the pre-pending of operations to the `Ecto.Multi`.

      Arguments and return value  as per the global `pre_apply` callback.
      """,
      type_spec: quote(do: pre_post_func()),
      type_doc: "`t:pre_post_func/0`"
    ],
    post_apply: [
      type: {:fun, 3},
      doc: """
      An optional callback that allows for the appending of operations to the `Ecto.Multi`.

      Arguments and return value as per the global `post_apply` callback.
      """,
      type_spec: quote(do: pre_post_func()),
      type_doc: "`t:pre_post_func/0`"
    ]
  ]
  @operation_options_schema NimbleOptions.new!(@operation_options)

  @type data() :: %{binary() => any()}
  @type mutation() :: %{required(binary()) => any()}
  @type operation() :: :insert | :update | :delete
  @operations [:insert, :update, :delete]
  @type txid() :: Transaction.id()
  @type context() :: %Context{
          index: non_neg_integer(),
          changes: Ecto.Mult.changes(),
          schema: Ecto.Schema.t(),
          operation: :insert | :update | :delete,
          callback: :load | :validate | :pre_apply | :post_apply
        }

  @type operation_opts() :: unquote([NimbleOptions.option_typespec(@operation_options_schema)])

  @operation_schema [
    type: :keyword_list,
    keys: @operation_options,
    doc: NimbleOptions.docs(NimbleOptions.new!(@operation_options)),
    type_spec: quote(do: operation_opts()),
    type_doc: "`t:operation_opts/0`"
  ]

  @parse_schema_options [
    format: [
      type: :atom,
      doc: """
      A module implementing the `#{inspect(__MODULE__.Format)}`
      behaviour.

      See `#{inspect(__MODULE__.Format)}`.
      """,
      type_spec: quote(do: Format.t()),
      type_doc: "[`Format.t()`](`t:#{inspect(Format)}.t/0`)"
    ],
    parser: [
      type: {:or, [{:fun, 1}, :mfa]},
      doc: """
      A function that parses some input data and returns a
      [`%Transaction{}`](`#{inspect(__MODULE__.Transaction)}`) struct or an error.
      See `c:#{inspect(__MODULE__.Format)}.parse_transaction/1`.
      """,
      type_doc: "`#{inspect(Format)}.parser_fun() | mfa()`",
      type_spec: quote(do: Format.parser_fun())
    ]
  ]
  @parse_schema NimbleOptions.new!(@parse_schema_options)

  @allow_schema NimbleOptions.new!(
                  table: [
                    type: {:or, [:string, {:list, :string}]},
                    doc: """
                    Override the table name of the `Ecto.Schema` struct to
                    allow for mapping between table names on the client and within Postgres.

                    If you pass just a table name, then any schema prefix in the client tables is ignored, so

                        Writer.allow(Todos, table: "todos")

                    will match client operations for `["public", "todos"]` and `["application", "todos"]` etc.

                    If you provide a 2-element list then the mapping will be exact and only
                    client relations matching the full `[schema, table]` pair will match the
                    given schema.

                        Writer.allow(Todos, table: ["public", "todos"])

                    Will match client operations for `["public", "todos"]` but
                    **not** `["application", "todos"]` etc.

                    Defaults to `Model.__schema__(:source)`, or if the Ecto schema
                    module has specified a `namespace` `[Model.__schema__(:prefix),
                    Model.__schema__(:source)]`.
                    """,
                    type_doc: "`String.t() | [String.t(), ...]`",
                    type_spec: quote(do: String.t() | [String.t(), ...])
                  ],
                  accept: [
                    type: {:list, {:in, @operations}},
                    doc: """
                    A list of actions to accept.

                    A transaction containing an operation not in the accept list will be rejected.

                    Defaults to accepting all operations, `#{inspect(@operations)}`.
                    """,
                    type_spec: quote(do: [operation(), ...])
                  ],
                  check: [
                    type: {:fun, 1},
                    doc: """
                    A function that validates every %#{inspect(__MODULE__.Operation)}{} in the transaction for correctness.

                    This is run before any database access is performed and so provides an
                    efficient way to prevent malicious writes without hitting your database.

                    Defaults to a function that allows all operations: `fn _ -> :ok end`.
                    """,
                    type_spec: quote(do: (Operation.t() -> :ok | {:error, term()}))
                  ],
                  before_all: [
                    type: {:fun, 1},
                    doc: """
                    Run only once (per transaction) after the parsing and `check` callback have
                    completed and before `load` and `validate` functions run.

                    Useful for pre-loading data from the database that can be shared across
                    all operation callbacks for all the mutations.

                    Arguments:

                    - `multi` an `Ecto.Multi` struct

                    Return value:

                    - `Ecto.Multi` struct with associated data

                    Defaults to no callback.
                    """,
                    type_spec: quote(do: (Ecto.Multi.t() -> Ecto.Multi.t()))
                  ],
                  load: [
                    type: {:or, [{:fun, 1}, {:fun, 2}]},
                    doc: """
                    A 1- or 2-arity function that accepts either the mutation
                    operation's data or an `Ecto.Repo` instance and the mutation data and
                    returns the original row from the database.

                    Arguments:

                    - `repo` the `Ecto.Repo` instance passed to `apply/4` or `transaction/3`
                    - `data` the original operation data

                    Valid return values are:

                    - `struct()` - an `Ecto.Schema` struct, that must match the
                      module passed to `allow/3`
                    - `{:ok, struct()}` - as above but wrapped in an `:ok` tuple
                    - `nil` - if no row matches the search criteria, or
                    - `{:error, String.t()}` - as `nil` but with a custom error string

                    A return value of `nil` or `{:error, reason}` will abort the transaction.

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
                  validate: [
                    type: {:or, [{:fun, 2}, {:fun, 3}]},
                    doc: """
                    a 2- or 3-arity function that returns an `Ecto.Changeset` for a given mutation.

                    ### Callback params

                    - `data` an Ecto.Schema struct matching the one used when
                      calling `allow/2` returned from the `load` function.
                    - `changes` a map of changes to apply to the `data`.
                    - `operation` (for 3-arity callbacks only) the operation
                      action, one of `:insert`, `:update` or `:delete`

                    At absolute minimum, this should call
                    `Ecto.Changeset.cast/3` to validate the proposed data:

                        def my_changeset(data, changes, _operation) do
                          Ecto.Changeset.cast(data, changes, @permitted_columns)
                        end

                    Defaults to the given model's `changeset/2` function if
                    defined, raises if no changeset function can be found.
                    """,
                    type_spec:
                      quote(
                        do:
                          (Ecto.Schema.t(), data() -> Ecto.Changeset.t())
                          | (Ecto.Schema.t(), data(), operation() -> Ecto.Changeset.t())
                      )
                  ],
                  pre_apply: [
                    type: {:fun, 3},
                    doc: """
                    an optional callback that allows for the pre-pending of
                    operations to the `Ecto.Multi` representing a mutation transaction.

                    If should be a 3-arity function.

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

                        def pre_apply(multi, changeset, context) do
                          name = Phoenix.Sync.Writer.operation_name(context, :event_insert)
                          Ecto.Multi.insert(multi, name, %Event{todo_id: id})
                        end

                    Defaults to no `nil`.
                    """,
                    type_spec: quote(do: pre_post_func()),
                    type_doc: "`t:pre_post_func/0`"
                  ],
                  post_apply: [
                    type: {:fun, 3},
                    doc: """
                    an optional callback function that allows for the
                    appending of operations to the `Ecto.Multi` representing a mutation
                    transaction.

                    See the docs for `:pre_apply` for the function signature and arguments.

                    Defaults to no `nil`.
                    """,
                    type_spec: quote(do: pre_post_func()),
                    type_doc: "`t:pre_post_func/0`"
                  ],
                  insert:
                    Keyword.put(@operation_schema, :doc, """
                    Callbacks for validating and modifying `insert` operations.

                    Accepts definitions for the `validate`, `pre_apply` and
                    `post_apply` functions for `insert` operations that will override the
                    top-level equivalents.

                    See the documentation for `allow/3`.

                    The only difference with these callback functions is that
                    the `action` parameter is redundant and therefore not passed.

                    Defaults to `[]`, using the top-level functions for all operations.
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
                      &Keyword.put(&1, :validate, type: {:or, [{:fun, 1}, {:fun, 2}]})
                    )
                    |> Keyword.put(:doc, """
                    Callbacks for validating and modifying `delete` operations.
                    See the documentation for `insert`.
                    """)
                )

  defstruct ingest: [], mappings: %{}

  @type allow_opts() :: [unquote(NimbleOptions.option_typespec(@allow_schema))]
  @type parse_opts() :: [unquote(NimbleOptions.option_typespec(@parse_schema))]
  @type schema_config() :: %{required(atom()) => term()}
  @type ingest_change() :: {Format.t(), Format.parser_fun(), Format.transaction_data()}
  @type repo_transaction_opts() :: keyword()
  @type transact_opts() :: [parse_opts() | repo_transaction_opts()]

  @type t() :: %__MODULE__{
          ingest: [ingest_change()],
          mappings: %{(binary() | [binary(), ...]) => schema_config()}
        }

  @doc """
  Create a new empty writer.

  Empty writers will reject writes to any tables. You should configure writes
  to the permitted tables by calling `allow/3`.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{}
  end

  @doc """
  Allow writes to the given `Ecto.Schema`.

  Only tables specified in calls to `allow/3` will be accepted by later calls
  to `transaction/3`. Any changes to tables not explicitly defined by `allow/3` calls
  will be rejected and cause the entire transaction to be rejected.

  ## Examples

      # allow writes to the Todo table using
      # `MyApp.Todos.Todo.check_mutation/1` to validate operations
      Phoenix.Sync.Writer.new()
      |> Phoenix.Sync.Writer.allow(
        MyApp.Todos.Todo,
        check: &MyApp.Todos.check_mutation/1
      )

      # A more complex configuration adding an `post_apply` callback to inserts
      # and using a custom query to load the original database value.
      Phoenix.Sync.Writer.new()
      |> Phoenix.Sync.Writer.allow(
        MyApp.Todos..Todo,
        load: &MyApp.Todos.get_for_mutation/1,
        check: &MyApp.Todos.check_mutation/1,
        insert: [
          post_apply: &MyApp.Todos.post_apply_insert_mutation/3
        ]
      )

  ## Supported options

  #{NimbleOptions.docs(@allow_schema)}

  """
  @spec allow(t(), module(), allow_opts()) :: t()
  def allow(writer, schema, opts \\ [])

  def allow(%__MODULE__{} = writer, schema, opts) when is_atom(schema) do
    {schema, table, pks} = validate_schema!(schema)

    config = NimbleOptions.validate!(opts, @allow_schema)

    key = config[:table] || table
    load_fun = load_fun(schema, pks, opts)
    check_fun = check_fun(opts)

    accept = Keyword.get(config, :accept, @operations) |> MapSet.new()

    table_config = %{
      schema: schema,
      table: table,
      pks: pks,
      accept: accept,
      check: check_fun
    }

    table_config =
      Enum.reduce(@operations, table_config, fn action, table_config ->
        Map.put(
          table_config,
          action,
          action_config(schema, config, action, load: load_fun, table: key, pks: pks)
        )
      end)

    Map.update!(writer, :mappings, &Map.put(&1, key, table_config))
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
    validate_fun =
      get_in(config, [action, :validate]) || config[:validate] || default_changeset!(schema) ||
        raise(ArgumentError, message: "No validate/3 or validate/2 defined for #{action}s")

    # nil-hooks are just ignored
    pre_apply_fun = get_in(config, [action, :pre_apply]) || config[:pre_apply]
    post_apply_fun = get_in(config, [action, :post_apply]) || config[:post_apply]

    before_all_fun = config[:before_all]

    Map.merge(
      Map.new(extra),
      %{
        schema: schema,
        before_all: before_all_fun,
        validate: validate_fun,
        pre_apply: pre_apply_fun,
        post_apply: post_apply_fun
      }
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

  defp check_fun(opts) do
    case opts[:check] do
      nil ->
        fn _ -> :ok end

      fun1 when is_function(fun1, 1) ->
        fun1

      fun when is_function(fun) ->
        info = Function.info(fun)

        raise ArgumentError,
          message:
            "Invalid check function. Expected a 1-arity function but got arity #{info[:arity]}"

      invalid ->
        raise ArgumentError,
          message:
            "Invalid check function. Expected a 1-arity function but got #{inspect(invalid)}"
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
  Add the given changes to the operations that will be applied within a `transaction/3`.

  Examples:

      {:ok, txid} =
        #{inspect(__MODULE__)}.new()
        |> #{inspect(__MODULE__)}.allow(MyApp.Todo)
        |> #{inspect(__MODULE__)}.ingest(changes, format: MyApp.MutationFormat)
        |> #{inspect(__MODULE__)}.ingest(other_changes, parser: &MyApp.MutationFormat.parse_other/1)
        |> #{inspect(__MODULE__)}.ingest(more_changes, parser: {MyApp.MutationFormat, :parse_more, []})
        |> #{inspect(__MODULE__)}.transaction(MyApp.Repo)

  Supported options:

  #{NimbleOptions.docs(@parse_schema)}
  """
  @spec ingest(t(), Format.transaction_data(), parse_opts()) :: t()
  def ingest(writer, changes, opts) do
    case validate_ingest_opts(opts) do
      {:ok, format, parser_fun} ->
        %{writer | ingest: [{format, parser_fun, changes} | writer.ingest]}

      {:error, message} ->
        raise Error, message: message
    end
  end

  @doc """
  Ingest and write changes to the given repo in a single call.

      #{inspect(__MODULE__)}.new()
      |> #{inspect(__MODULE__)}.apply(changes, Repo, parser: &MyFormat.parse/1)

  is equivalent to:

      #{inspect(__MODULE__)}.new()
      |> #{inspect(__MODULE__)}.ingest(changes, parser: &MyFormat.parse/1)
      |> #{inspect(__MODULE__)}.transaction(Repo)
  """
  @spec apply(t(), Format.transaction_data(), Ecto.Repo.t(), transact_opts()) ::
          {:ok, txid(), Ecto.Multi.changes()} | Ecto.Multi.failure()
  def apply(%__MODULE__{} = writer, changes, repo, opts)
      when is_atom(repo) and is_list(changes) do
    {writer_opts, txn_opts} = split_writer_txn_opts(opts)

    writer
    |> ingest(changes, writer_opts)
    |> transaction(repo, txn_opts)
  end

  @writer_option_keys Keyword.keys(@parse_schema_options)

  defp split_writer_txn_opts(opts) do
    Keyword.split(opts, @writer_option_keys)
  end

  @doc """
  Given a writer configuration created using `allow/3` translate the list of
  mutations into an `Ecto.Multi` operation.

  Example:

      %Ecto.Multi{} = multi =
        #{inspect(__MODULE__)}.new()
        |> #{inspect(__MODULE__)}.allow(MyApp.Todos.Todo, check: &my_check_function/1)
        |> #{inspect(__MODULE__)}.allow(MyApp.Options.Option, check: &my_check_function/1)
        |> #{inspect(__MODULE__)}.ingest(changes, format: #{inspect(__MODULE__.Format.TanstackOptimistic)})
        |> #{inspect(__MODULE__)}.to_multi()

  If you want to add extra operations to the mutation transaction, beyond those
  applied by any `pre_apply` or `post_apply` callbacks in your mutation config then use
  the functions in `Ecto.Multi` to do those as normal.

  Use `transaction/3` to apply the changes to the database and return the
  transaction id.

  `to_multi/1` builds an `Ecto.Multi` struct containing the operations required to
  write the mutation operations to the database.

  The order of operation is:

  ### 1. Parse

  The transaction data is parsed, using either the `format` or the `parser` function
  supplied in `ingest/3`.

  ### 2. Check

  The user input data in each operation in the transaction is tested for validity
  via the `check` function.

  At this point no database operations have taken place. Errors at the parse or
  `check` stage result in an early exit. The purpose of the `check` callback is
  sanity check the incoming mutation data against basic sanitization rules, much
  as you would do with `Plug` middleware and controller params pattern matching.

  Now that we have a list of validated mutation operations, the next step is:

  ### 3. Before-all

  Perform any actions defined in the `before_all` callback.

  This only happens once per transaction, the first time the model owning the
  callback is included in the operation list.

  The following actions happen once per operation in the transaction:

  ### 4. Load

  The `load` function is called to retrieve the source row from the database
  (for `update` and `delete` operations), or the schema's `__struct__/0`
  function is called to instantiate an empty struct (`insert`).

  ### 5. Validate

  The `validate` function is called with the result of the `load` function
  and the operation's changes.

  ### 6. Pre-apply

  The `pre_apply` callback is called with a `multi` instance, the result of the
  `validate` function and the current `Context`. The result is
  [merged](`Ecto.Multi.merge/2`) into the transaction's ongoing `Ecto.Multi`.

  ### 7. Apply

  The actual operation is applied to the database using one of
  `Ecto.Multi.insert/4`, `Ecto.Multi.update/4` or `Ecto.Multi.delete/4`, and

  ### 8. Post-apply

  Finally the `post_apply` callback is called.

  Any error in any of these stages will abort the entire transaction and leave
  your database untouched.
  """
  @spec to_multi(t()) :: Ecto.Multi.t()
  def to_multi(%__MODULE__{} = writer) do
    # Want to return a multi here but have that multi fail without contacting
    # the db if any of the check calls fail.
    #
    # Looking at `Ecto.Multi.__apply__/4` it first checks for invalid
    # operations before doing anything. So i can just add an error to a blank
    # multi and return that and the transaction step will fail before touching
    # the repo.
    writer.ingest
    |> Enum.reverse()
    |> Enum.reduce_while(start_multi(), fn {_format, parser_fun, changes}, multi ->
      with {:ok, %Transaction{} = txn} <- parse_check(writer, parser_fun, changes) do
        {:cont, Enum.reduce(txn.operations, multi, &append_multi(&2, &1, writer))}
      else
        {step, {:error, error}} ->
          {:halt, Ecto.Multi.error(Ecto.Multi.new(), step, error)}
      end
    end)
  end

  @doc """
  Ingest changes and map them into an `Ecto.Multi` instance ready to apply
  using `transaction/3` or `c:Ecto.Repo.transaction/2`

  This is a wrapper around `ingest/3` and `to_multi/1`.

  Example:

      %Ecto.Multi{} = multi =
        #{inspect(__MODULE__)}.new()
        |> #{inspect(__MODULE__)}.allow(MyApp.Todos.Todo, check: &my_check_function/1)
        |> #{inspect(__MODULE__)}.allow(MyApp.Options.Option, check: &my_check_function/1)
        |> #{inspect(__MODULE__)}.to_multi(changes, format: #{inspect(__MODULE__.Format.TanstackOptimistic)})

  """
  @spec to_multi(t(), Format.transaction_data(), parse_opts()) :: Ecto.Multi.t()
  def to_multi(%__MODULE__{} = writer, changes, opts) do
    writer
    |> ingest(changes, opts)
    |> to_multi()
  end

  defp parse_check(writer, parser_fun, changes) do
    with {:parse, {:ok, %Transaction{} = txn}} <- {:parse, parser_fun.(changes)},
         {:check, :ok} <- {:check, check_transaction(writer, txn)} do
      {:ok, txn}
    end
  end

  @doc """
  Use the parser configured in the given [`Writer`](`#{inspect(__MODULE__)}`)
  instance to decode the given transaction data.

  This can be used to handle mutation operations explicitly:

      {:ok, txn} = #{inspect(__MODULE__)}.parse_transaction(my_json_tx_data, format: #{inspect(__MODULE__.Format.TanstackOptimistic)})

      {:ok, txid} =
        Repo.transaction(fn ->
          Enum.each(txn.operations, fn operation ->
            # do something wih the given operation
            # raise if something is wrong...
          end)
          # return the transaction id
          #{inspect(__MODULE__)}.txid!(Repo)
        end)
  """
  @spec parse_transaction(Format.transaction_data(), parse_opts()) ::
          {:ok, Transaction.t()} | {:error, term()}
  def parse_transaction(changes, opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @parse_schema),
         {:ok, _format, parser_fun} <- validate_ingest_opts(opts) do
      parser_fun.(changes)
    end
  end

  defp validate_ingest_opts(opts) do
    with {:ok, config} <- NimbleOptions.validate(opts, @parse_schema),
         format = Keyword.get(config, :format),
         parser = Keyword.get(config, :parser),
         {:ok, parser_func} <- parser_fun(format, parser) do
      {:ok, format, parser_func}
    end
  end

  defp parser_fun(format, parser) do
    case format_parser(format) do
      parser when is_function(parser, 1) ->
        {:ok, parser}

      nil ->
        case parser do
          nil ->
            {:error, "no valid format or parser"}

          parser when is_function(parser, 1) ->
            {:ok, parser}

          {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) ->
            {:ok, fn changes -> Kernel.apply(m, f, [changes | a]) end}
        end

      {:error, _} = error ->
        error
    end
  end

  defp format_parser(nil), do: nil

  defp format_parser(format) when is_atom(format) do
    if Code.ensure_loaded?(format) && function_exported?(format, :parse_transaction, 1) do
      Function.capture(format, :parse_transaction, 1)
    else
      {:error,
       "#{inspect(format)} does not implement the #{inspect(__MODULE__.Format)} behaviour"}
    end
  end

  defp check_transaction(%__MODULE__{} = writer, %Transaction{} = txn) do
    Enum.reduce_while(txn.operations, :ok, fn operation, :ok ->
      case mutation_actions(operation, writer) do
        {:ok, actions} ->
          %{check: check, accept: accept} = actions

          if MapSet.member?(accept, operation.operation) do
            case check.(operation) do
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

  @txid_name {:__phoenix_sync__, :txid}
  @txid_query "SELECT txid_current() as txid"

  defp start_multi do
    Ecto.Multi.new()
    |> txid_step()
  end

  defp txid_step(multi \\ Ecto.Multi.new()) do
    Ecto.Multi.run(multi, @txid_name, fn repo, _ ->
      case repo.__adapter__() do
        Ecto.Adapters.Postgres ->
          txid(repo)

        adapter ->
          Logger.warning("Unsupported adapter #{adapter}. txid will be nil")
          {:ok, nil}
      end
    end)
  end

  defp has_txid_step?(multi) do
    multi
    |> Ecto.Multi.to_list()
    |> Enum.any?(fn {name, _} -> name == @txid_name end)
  end

  defp append_multi(multi, %Operation{} = op, %__MODULE__{} = writer) do
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
      |> apply_before_all(action)
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

  defp apply_before_all(multi, %{before_all: nil} = _action) do
    multi
  end

  defp apply_before_all(multi, %{before_all: before_all_fun} = action)
       when is_function(before_all_fun, 1) do
    key = {action.schema, :before_all}

    Ecto.Multi.merge(multi, fn
      %{^key => true} = _changes ->
        Ecto.Multi.new()

      _changes ->
        before_all_fun.(Ecto.Multi.new())
        |> Ecto.Multi.put({action.schema, :before_all}, true)
    end)
  end

  defp mutation_changeset(multi, %Operation{} = op, %Context{} = ctx, action) do
    %{schema: schema, validate: changeset_fun} = action
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
        {:ok, fun3.(data, change_data, op.operation)}

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

  defp apply_before(multi, operation, ctx, %{pre_apply: pre_apply_fun} = action) do
    apply_hook(multi, operation, ctx, {:pre_apply, pre_apply_fun}, action)
  end

  defp apply_after(multi, operation, ctx, %{post_apply: post_apply_fun} = action) do
    apply_hook(multi, operation, ctx, {:post_apply, post_apply_fun}, action)
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
          raise "Invalid #{hook_name} for #{inspect(action.table)} #{inspect(operation.operation)}"
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

        Phoenix.Sync.Writer.new()
        |> Phoenix.Sync.Writer.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Writer.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Writer.ingest(
          changes,
          format: Phoenix.Sync.Writer.Format.TanstackOptimistic
        )
        |> Phoenix.Sync.Writer.transaction(MyApp.Repo)
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

  Also supports normal fun/0 or fun/1 style transactions much like
  `c:Ecto.Repo.transaction/2`, returning the txid of the operation:

      {:ok, txid, todo} =
        Phoenix.Sync.Writer.transaction(fn ->
          Repo.insert!(changeset)
        end, Repo)
  """
  @spec transaction(t() | Ecto.Multi.t(), Ecto.Repo.t(), keyword()) ::
          {:ok, txid(), Ecto.Multi.changes()} | Ecto.Multi.failure()
  def transaction(writer_or_multi, repo, opts \\ [])

  def transaction(%__MODULE__{ingest: []}, _repo, _opts) do
    {:error, "no changes ingested"}
  end

  def transaction(%__MODULE__{} = writer, repo, opts) do
    writer
    |> to_multi()
    |> transaction(repo, opts)
  end

  def transaction(%Ecto.Multi{} = multi, repo, opts) when is_atom(repo) do
    wrapped_multi =
      if has_txid_step?(multi) do
        multi
      else
        Ecto.Multi.prepend(multi, txid_step())
      end

    with {:ok, changes} <- repo.transaction(wrapped_multi, opts) do
      {txid, changes} = Map.pop!(changes, @txid_name)
      {:ok, txid, changes}
    end
  end

  @doc """
  Apply operations from a mutation directly via a transaction.

  `operation_fun` is a 1-arity function that receives each of the
  `%#{inspect(__MODULE__.Operation)}{}` structs within the mutation data and
  should apply them appropriately. It should return `:ok` or `{:ok, result}` if
  successful or `{:error, reason}` if the operation is invalid or failed to
  apply. If any operation returns `{:error, _}` or raises then the entire
  transaction is aborted.

  This function will return `{:error, reason}` if the transaction data fails to parse.

      {:ok, txid} =
        #{inspect(__MODULE__)}.transact(
          my_encoded_txn,
          MyApp.Repo,
          fn
            %{operation: :insert, relation: [_, "todos"], change: change} ->
              MyApp.Repo.insert(...)
            %{operation: :update, relation: [_, "todos"], data: data, change: change} ->
              MyApp.Repo.update(Ecto.Changeset.cast(...))
            %{operation: :delete, relation: [_, "todos"], data: data} ->
              # we don't allow deletes...
              {:error, "invalid delete"}
          end,
          format: #{inspect(__MODULE__.Format.TanstackOptimistic)},
          timeout: 60_000
        )

  Any of the `opts` not used by this module are passed onto the
  `c:Ecto.Repo.transaction/2` call.

  This is equivalent to the below:

      {:ok, txn} =
        #{inspect(__MODULE__)}.parse_transaction(
          my_encoded_txn,
          format: #{inspect(__MODULE__.Format.TanstackOptimistic)}
        )

      {:ok, txid} =
        MyApp.Repo.transaction(fn ->
          Enum.each(txn.operations, fn
            %{operation: :insert, relation: [_, "todos"], change: change} ->
              # insert a Todo
            %{operation: :update, relation: [_, "todos"], data: data, change: change} ->
              # update a Todo
            %{operation: :delete, relation: [_, "todos"], data: data} ->
              # we don't allow deletes...
              raise "invalid delete"
          end)
          #{inspect(__MODULE__)}.txid!(MyApp.Repo)
        end, timeout: 60_000)
  """
  @spec transact(
          Format.transaction_data(),
          Ecto.Repo.t(),
          operation_fun :: (Operation.t() -> :ok | {:ok, any()} | {:error, any()}),
          transact_opts()
        ) :: {:ok, txid()} | {:error, any()}
  def transact(changes, repo, operation_fun, opts)
      when is_function(operation_fun, 1) and is_atom(repo) do
    {parse_opts, txn_opts} = split_writer_txn_opts(opts)

    with {:ok, %Transaction{} = txn} <- parse_transaction(changes, parse_opts) do
      repo.transaction(
        fn ->
          Enum.reduce_while(txn.operations, :ok, fn op, :ok ->
            case operation_fun.(op) do
              {:ok, _result} ->
                {:cont, :ok}

              :ok ->
                {:cont, :ok}

              {:error, _reason} = error ->
                {:halt, error}

              other ->
                raise ArgumentError,
                      "expected to return :ok, {:ok, _} or {:error, _}, got: #{inspect(other)}"
            end
          end)
          |> case do
            {:error, reason} ->
              repo.rollback(reason)

            :ok ->
              txid!(repo)
          end
        end,
        txn_opts
      )
    end
  end

  @doc """
  Extract the transaction id from changes or from a `Ecto.Repo` within a
  transaction.

  This allows you to use a standard `c:Ecto.Repo.transaction/2` call to apply
  mutations defined using `apply/2` and extract the transaction id afterwards.

  Example

      {:ok, changes} =
        Phoenix.Sync.Writer.new()
        |> Phoenix.Sync.Writer.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Writer.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Writer.to_multi(changes, format: Phoenix.Sync.Writer.Format.TanstackOptimistic)
        |> MyApp.Repo.transaction()

      {:ok, txid} = Phoenix.Sync.Writer.txid(changes)

  It also allows you to get a transaction id from any active transaction:

      MyApp.Repo.transaction(fn ->
        {:ok, txid} = #{inspect(__MODULE__)}.txid(MyApp.Repo)
      end)

  Attempting to run `txid/1` on a repo outside a transaction will return an
  error.
  """
  @spec txid(Ecto.Multi.changes()) :: {:ok, txid()} | :error
  def txid(%{@txid_name => txid} = _changes), do: {:ok, txid}
  def txid(changes) when is_map(changes), do: :error

  def txid(repo) when is_atom(repo) do
    if repo.in_transaction?() do
      with {:ok, %{rows: [[txid]]}} = repo.query(@txid_query) do
        {:ok, txid}
      end
    else
      {:error, %Error{message: "not in a transaction"}}
    end
  end

  @doc """
  Returns the a transaction id or raises on an error.

  See `txid/1`.
  """
  @spec txid!(Ecto.Multi.changes()) :: txid()
  def txid!(%{@txid_name => txid} = _changes), do: txid
  def txid!(%{}), do: raise(ArgumentError, message: "No txid in change data")

  def txid!(repo) when is_atom(repo) do
    case txid(repo) do
      {:ok, txid} -> txid
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Return a unique operation name for use in `pre_apply` or `post_apply` callbacks.

  `Ecto.Multi` requires that all operation names be unique within a
  transaction. This function gives you a simple way to generate a name for your
  own operations that is guarateed not to conflict with any other.

  Example:

      Phoenix.Sync.Writer.new(format: Phoenix.Sync.Writer.Format.TanstackOptimistic)
      |> Phoenix.Sync.Writer.allow(
        MyModel,
        pre_apply: fn multi, changeset, context ->
          name = Phoenix.Sync.Writer.operation_name(context)
          Ecto.Multi.insert(multi name, AuditEvent.for_changeset(changeset))
        end
      )
  """
  def operation_name(%Context{} = ctx) do
    {ctx.schema, ctx.operation.operation, ctx.index}
  end

  @doc """
  Like `operation_name/1` but allows for a custom label.
  """
  @spec operation_name(context(), term()) :: term()
  def operation_name(%Context{} = ctx, label) do
    {operation_name(ctx), label}
  end

  defp load_key(ctx, pk) do
    opkey(schema: ctx.schema, operation: ctx.operation.operation, index: ctx.index, pk: pk)
  end
end
