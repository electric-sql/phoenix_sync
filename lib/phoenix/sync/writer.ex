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
            Phoenix.Sync.Writer.new(format: Format.TanstackOptimistic)
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
            |> Phoenix.Sync.Writer.apply(transaction, Repo)

          render(conn, :mutations, txid: txid)
        end
      end

  The controller constructs a `#{inspect(__MODULE__)}` instance and pipes it
  through a series of [`Writer.allow/3`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Writer.html#allow/3)
  calls, registering functions against `Ecto.Schema`s (in this case `Projects.Project`
  and `Projects.Issue`) to validate and authorize each of these mutation operations
  before applying them as a single transaction.

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

  The `{:ok, txid, changes}` return value from `Phoenix.Sync.Writer.apply/3`
  allows the Postgres transaction ID to be returned to the client in the response data.

  This allows clients to monitor the read-path sync stream and match on the
  arrival of the same transaction id. When the client receives this transaction id
  back through its sync stream, it knows that it can discard the local optimistic
  state for that transaction. (This is a more robust way of managing optimistic state
  than just matching on instance IDs, as it allows for local changes to be rebased
  on concurrent changes to the same date from other users).

  `#{inspect(__MODULE__)}` uses `Ecto.Multi`'s transaction update mechanism
  under the hood, which means that either all the operations in a client
  transaction are accepted or none are. See `apply/2` for how you can hook
  into the `Ecto.Multi` after applying your change data.

  ## Client Libraries

  `#{inspect(__MODULE__)}` is not coupled to any particular client-side implementation.
  See Electric's [write pattern guides and example code](https://electric-sql.com/docs/guides/writes)
  for implementation strategies and examples.

  Instead, `#{inspect(__MODULE__)}` provides an adapter pattern where you can register
  a `format` adapter to parse the expected payload format from a client side library
  into the struct that `#{inspect(__MODULE__)}` expects.

  The currently supported format adapters are:

  - [TanStack/optimistic](https://github.com/TanStack/optimistic) "A library
    for creating fast optimistic updates with flexible backend support that pairs
    seamlessly with sync engines"

    Integration:

        #{inspect(__MODULE__)}.new(format: #{inspect(__MODULE__.Format.TanstackOptimistic)})

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

  Calling `new/1` creates an empty writer configuration with the given mutation
  parser. But this alone does not permit any mutations. In order to allow writes
  from clients you must call `allow/3` with a schema module and some callback functions.

      # create an empty writer configuration which accepts writes in the given format
      writer = #{inspect(__MODULE__)}.new(format: #{inspect(__MODULE__.Format.TanstackOptimistic)})

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
            Writer.new(format: Writer.Format.TanstackOptimistic)
            |> Writer.allow(
              Todos.Todo,
              check: &validate_mutation(&1, user_id),
              load: &fetch_for_user(&1, user_id),
            )
            |> Writer.apply(transaction, Repo)

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

  Because the result of `apply/2` is an `Ecto.Multi` instance you can also just
  append operations using the normal `Ecto.Multi` functions:

      {:ok, txid, _changes} =
        Writer.new(parser: &my_transaction_parser/1)
        |> Writer.allow(Todo, ...)
        |> Writer.apply(transaction)
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

  @writer_schema NimbleOptions.new!(
                   format: [
                     type: :atom,
                     doc: """
                     A module implementing the `#{inspect(__MODULE__.Format)}`
                     behaviour or a function that takes mutation data from the client and
                     returns a [`%Transaction{}`](`#{inspect(__MODULE__.Transaction)}`) struct.

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
                     type_spec: quote(do: Format.parser_fun())
                   ]
                 )

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
                    A function for check checks that validate the parameters passed to the
                    pending %#{inspect(__MODULE__.Operation)}{}.

                    This is run before any database access is performed and so provides an
                    efficient way to prevent malicious writes without hitting your database.

                    Defaults to a function that allows all operations: `fn _ -> :ok end`.
                    """,
                    type_spec: quote(do: (Operation.t() -> :ok | {:error, term()}))
                  ],
                  before_all: [
                    type: {:fun, 1},
                    doc: """
                    Run only once (per transaction) after the parsing and check checks have
                    completed and before load and validation functions run.

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

  defstruct format: nil, parser: nil, mappings: %{}

  @type writer_opts() :: [unquote(NimbleOptions.option_typespec(@writer_schema))]
  @type allow_opts() :: [unquote(NimbleOptions.option_typespec(@allow_schema))]
  @type schema_config() :: %{required(atom()) => term()}

  @type t() :: %__MODULE__{
          format: module() | nil,
          parser: Format.parser_fun(),
          mappings: %{(binary() | [binary(), ...]) => schema_config()}
        }

  @doc """
  Create a new empty writer with the given global options.

  Supported options:

  #{NimbleOptions.docs(@writer_schema)}

  You can either pass a `parser` function or a `format`.

  Empty writers will reject writes to any tables. You should configure writes
  to the permitted tables by calling `allow/3`.
  """
  def new(opts \\ [])

  @spec new(writer_opts()) :: t()
  def new(opts) when is_list(opts) do
    config = NimbleOptions.validate!(opts, @writer_schema)

    format = Keyword.get(config, :format)

    parser_func =
      Keyword.get(config, :parser) || format_parser(format) ||
        raise ArgumentError, message: "Missing `:format` or `:parser` configuration"

    %__MODULE__{format: format, parser: parser_func}
  end

  defp format_parser(nil), do: nil

  defp format_parser(format) when is_atom(format) do
    if Code.ensure_loaded?(format) && function_exported?(format, :parse_transaction, 1) do
      Function.capture(format, :parse_transaction, 1)
    else
      raise ArgumentError,
        message:
          "#{inspect(format)} does not implement the #{inspect(__MODULE__.Format)} behaviour"
    end
  end

  @doc """
  Allow writes to the given `Ecto.Schema`.

  Only tables specified in calls to `allow/3` will be accepted by later calls
  to `apply/2`. Any changes to tables not explicitly defined by `allow/3` calls
  will be rejected and cause the entire transaction to be rejected.

  ## Examples

      # allow writes to the Todo table using
      # `MyApp.Todos.Todo.check_mutation/1` to validate operations
      Phoenix.Sync.Writer.new(format: MyApp.OperationFormat)
      |> Phoenix.Sync.Writer.allow(
        MyApp.Todos.Todo,
        check: &MyApp.Todos.check_mutation/1
      )

      # A more complex configuration adding an `post_apply` callback to inserts
      # and using a custom query to load the original database value.
      Phoenix.Sync.Writer.new(format: MyApp.OperationFormat)
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

  def allow(%__MODULE__{} = write, schema, opts) when is_atom(schema) do
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
            "Invalid check function. Expected a 1-arity function but #{inspect(invalid)}"
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
  @spec apply(t(), Format.transaction_data(), Ecto.Repo.t()) ::
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
        |> #{inspect(__MODULE__)}.allow(MyApp.Todos.Todo, check: &my_check_function/1)
        |> #{inspect(__MODULE__)}.allow(MyApp.Options.Option, check: &my_check_function/1)
        |> #{inspect(__MODULE__)}.apply(changes)

  If you want to add extra operations to the mutation transaction, beyond those
  applied by any `pre_apply` or `post_apply` callbacks in your mutation config then use
  the functions in `Ecto.Multi` to do those as normal.

  Use `transaction/3` to apply the changes to the database and return the
  transaction id.

  `apply/2` builds an `Ecto.Multi` struct containing the operations required to
  write the mutation operations to the database.

   The order of operation is:

  ### 1. Parse

  The transaction data is parsed, using either the `format` or the `parser` function
  supplied in `new/1`.

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
  @spec apply(t(), Format.transaction_data()) :: Ecto.Multi.t()
  def apply(%__MODULE__{} = writer, changes) do
    # Want to return a multi here but have that multi fail without contacting
    # the db if any of the check calls fail.
    #
    # Looking at `Ecto.Multi.__apply__/4` it first checks for invalid
    # operations before doing anything. So i can just add an error to a blank
    # multi and return that and the transaction step will fail before touching
    # the repo.
    with {:parse, {:ok, %Transaction{} = txn}} <- {:parse, parse_transaction(writer, changes)},
         {:check, :ok} <- {:check, check_transaction(writer, txn)} do
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
    case writer.parser do
      fun when is_function(fun, 1) ->
        fun.(changes)

      {m, f, a} when is_atom(m) and is_atom(f) and is_list(a) ->
        Kernel.apply(m, f, [changes | a])
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

        Phoenix.Sync.Writer.new(format: Phoenix.Sync.Writer.Format.TanstackOptimistic)
        |> Phoenix.Sync.Writer.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Writer.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Writer.apply(changes)
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
        Phoenix.Sync.Writer.new(format: Phoenix.Sync.Writer.Format.TanstackOptimistic)
        |> Phoenix.Sync.Writer.allow(MyApp.Todos.Todo)
        |> Phoenix.Sync.Writer.allow(MyApp.Options.Option)
        |> Phoenix.Sync.Writer.apply(changes)
        |> MyApp.Repo.transaction()

      {:ok, txid} = Phoenix.Sync.Writer.txid(changes)
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

  # @doc """
  # ## TODO
  # """
  # @spec fetch_or_load(context(), map()) :: {:ok, Ecto.Schema.t()} | {:error, term()}
  # def fetch_or_load(%Context{} = ctx, _attrs) do
  #   dbg(ctx)
  #   :error
  # end

  defp load_key(ctx, pk) do
    opkey(schema: ctx.schema, operation: ctx.operation.operation, index: ctx.index, pk: pk)
  end
end
