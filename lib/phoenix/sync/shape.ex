defmodule Phoenix.Sync.Shape do
  @moduledoc """
  Materialize a shape stream into memory and subscribe to changes.

  Add a `#{inspect(__MODULE__)}` to your supervision tree to subscribe to a
  sync stream and maintain a synchronized local copy of the data in-memory.

      defmodule MyApp.Application do
        use Application

        def start(_type, _args) do
          children = [
            {Phoenix.Sync.Shape, [MyApp.Todo, name: MyApp.TodoShape]}
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end
      end

  Or use `start_link/1` or `start_link/2` to start a shape process manually:

      {:ok, pid} = #{inspect(__MODULE__)}.start_link(MyApp.Todo)

      {:ok, pid} = #{inspect(__MODULE__)}.start_link(MyApp.Todo, replica: :full)

  This allows you to subscribe to the shape and receive notifications and
  also retrieve the current dataset.

  `to_list/2` will return the current state of the shape as a list:

      #{inspect(__MODULE__)}.to_list(MyApp.TodoShape)
      [
        {~s|"public"."todos"/"1"|, %MyApp.Todo{id: 1}},
        {~s|"public"."todos"/"2"|, %MyApp.Todo{id: 2}},
        # ...
      ]

  `subscribe/2` registers the current process to receive change events (and
  `unsubscribe/1` allows you to stop receiving them):

      # use the registered `name` of the shape to receive change events
      # within a GenServer or other process
      def init(_args) do
        ref = #{inspect(__MODULE__)}.subscribe(MyApp.TodoShape)
        {:ok, ref}
      end

      # handle the shape events...
      def handle_info({:sync, ref, {operation, {key, _value}}, ref)
          when operation in [:insert, :update, :delete] do
        IO.inspect([{operation, key}])
        {:noreply, state}
      end

      def handle_info({:sync, ref, control}, ref)
          when control in [:up_to_date, :must_refetch] do
        IO.inspect(control: control)
        {:noreply, state}
      end

  ### Keys

  The sync stream gives every value in the source table a unique key which is
  defined as the full table name (with namespace) and the primary key value,
  e.g. `~s|"public"."todos"/"1"|`.

  If a table has a composite primary key, the key will include all primary key
  values. e.g. `~s|"public"."todos"/"1"/"123"|`.
  """

  @doc false
  use GenServer

  alias Phoenix.Sync.PredefinedShape
  alias Electric.Client.Message
  alias Phoenix.Sync.Shape

  @type shape() :: GenServer.server()
  @type key() :: String.t()
  @type value() :: term()
  @type tag() :: term()
  @type operation() :: :insert | :update | :delete
  @type control() :: :up_to_date | :must_refetch
  @type payload() :: {operation(), {key(), value()}} | control()
  @type event() :: {tag(), reference(), payload()}

  @type subscription_msg_type() :: operation() | control()
  @type subscribe_opts() :: [{:only, [subscription_msg_type()]} | {:tag, term()}]

  @type enum_opts() :: [{:keys, boolean()}]

  @type stream_options() :: [
          unquote(NimbleOptions.option_typespec(Phoenix.Sync.PredefinedShape.schema()))
          | {:name, GenServer.name()}
        ]
  @type shape_options() :: [
          Phoenix.Sync.queryable()
          | stream_options()
        ]

  @doc """
  Start a new shape process that will receive the sync stream events and
  maintain an in-memory copy of the dataset in sync with Postgres.

  ## Options

  Shapes are defined exactly as for `Phoenix.Sync.Client.stream/2`:

      # using an `Ecto.Schema` module
      {:ok, pid} = #{inspect(__MODULE__)}.start_link(MyApp.Todo)

      # or a full `Ecto.Query`
      {:ok, pid} = #{inspect(__MODULE__)}.start_link(
        from(t in MyApp.Todo, where: t.completed == true)
      )

      # we can pass extra sync options
      {:ok, pid} = #{inspect(__MODULE__)}.start_link(
        from(t in MyApp.Todo, where: t.completed == true),
        replica: :full
      )

  but also accept a `:name` much like other `GenServer` processes.

      {:ok, pid} = #{inspect(__MODULE__)}.start_link(MyApp.Todo, name: TodosShape)

  To start a Shape within a supervision tree, you pass the options as the child
  spec as if they were arguments:

      children = [
        {#{inspect(__MODULE__)}, [MyApp.Todo, name: TodoShape]}
      ]

  ## Options

  #{Electric.Client.ShapeDefinition.schema_definition() |> NimbleOptions.new!() |> NimbleOptions.docs()}
  """
  @spec start_link(shape_options()) :: GenServer.on_start()
  def start_link(args) do
    with {:ok, stream_args, shape_opts} <- validate_args(args) do
      GenServer.start_link(
        __MODULE__,
        {stream_args, shape_opts},
        Keyword.take(shape_opts, [:name])
      )
    end
  end

  @doc """
  See `start_link/1`.
  """
  @spec start_link(String.t() | PredefinedShape.queryable(), stream_options()) ::
          GenServer.on_start()
  def start_link(queryable, opts) do
    start_link([queryable | opts])
  end

  @doc """
  Subscribe the current process to the given shape and receive notifications
  about new changes on the stream.

      # create a shape that holds all the completed todos
      {:ok, shape} = Phoenix.Sync.Shape.start_link(from(t in Todo, where: t.completed == true))

      # subscribe this process to the shape events
      ref = Phoenix.Sync.Shape.subscribe(shape)

      receive do
        {:sync, ^ref, {:insert, {_key, todo}} ->
          # a todo has been marked as completed
        {:sync, ^ref, {:delete, {_key, todo}} ->
          # a todo has been deleted or moved from completed to not completed
        {:sync, ^ref, {:update, {_key, todo}} ->
          # the todo's title has been changed
        {:sync, ^ref, :up_to_date} ->
          # the local shape is fully synchronized with the remote server
        {:sync, ^ref, :must_refetch} ->
          # the local data has been invalidated and needs to be refetched
      end

  In the example above, changes in the `completed` state of a todo will move
  the affected item into or out of the shape because it's built using a filter
  that only allows completed todos. These moves into or out of the shape are
  mapped to `:insert` and `:delete` events, respectively.

  The message format is `{tag, reference, payload}`

  - `tag` defaults to `:sync` but can be customized using the `:tag` option (see below).

  - `reference` is the unique reference for the subscription returned by `subscribe/2`.

  - `type` is the operation type, which can be one of `:insert`, `:update`,
    `:delete`, `:up_to_date`, or `:must_refetch`.
    - `:up_to_date` indicates that the shape is fully synchronized.
    - `:must_refetch` indicates that the shape needs to be refetched due to a
    schema change, table truncation or some other server-side change.

  - `payload` depends on the operation type:
    - For `:insert`, `:update` and `:delete`, it is a tuple `{operation, {key, value}}` where `key` is a unique
      identifier for the value and `value` is the actual data.
    - For `:up_to_date` and `:must_refetch`, it is just the operation.

  ## Options

  - `only` - Limit events to only a subset of the available message types  (see
    above). If not provided, all message types will be sent. If you want to
    receive all `:insert`, `:update` and `:delete` events, you can use `only:
    :changes`.

  - `tag` - change the tag of the message sent to subscriber. Defaults to `:sync`.

  ## Examples

      # only receive notification when the shape is fully synchronized
      ref = Phoenix.Sync.Shape.subscribe(query, only: [:up_do_date])

      # only receive notification of deletes
      ref = Phoenix.Sync.Shape.subscribe(query, only: [:delete])

      # only receive notification when the shape has been invalidated and needs
      # to be refetched. Give the message a custom tag to make it easier to
      # identify the originating shape.
      ref = Phoenix.Sync.Shape.subscribe(Todo, only: [:must_refetch], tag: {:sync, :todo})
  """
  @spec subscribe(shape(), subscribe_opts()) :: reference()
  def subscribe(shape, opts \\ []) do
    GenServer.call(shape, {:subscribe, opts})
  end

  @doc """
  Unsubscribe the current process from the given shape.
  """
  @spec unsubscribe(shape()) :: :ok
  def unsubscribe(shape) do
    GenServer.call(shape, :unsubscribe)
  end

  @doc """
  Get a list of the current subscribers to the given shape.
  """
  @spec subscribers(shape()) :: [pid()]
  def subscribers(shape) do
    GenServer.call(shape, :subscribers)
  end

  @doc """
  Get all values in the shape.

  Returns a list of `{key, value}` tuples, where `key` is a unique identifier
  for the value.

  ## Options

  - `keys` (default: `true`) - if set to false, returns only the values without keys.
  """
  @spec to_list(shape(), opts :: enum_opts()) :: [{key(), value()}] | [value()]
  def to_list(shape, opts \\ []) do
    shape
    |> whereis!()
    |> :ets.match_object(:_)
    |> map_all(Keyword.get(opts, :keys, true))
  end

  defp map_all(rows, true), do: rows
  defp map_all(rows, false), do: Enum.map(rows, &elem(&1, 1))

  @doc """
  Return a lazy stream of the current values in the shape.


      {:ok, _pid} = Phoenix.Sync.Shape.start_link(MyApp.Todo, name: :todos)

      stream = Phoenix.Sync.Shape.stream(:todos)

      Enum.into(stream, %{})
      %{
        ~s|"public"."todos"/"1"| => %MyApp.Todo{id: 1},
        ~s|"public"."todos"/"2"| => %MyApp.Todo{id: 2},
        # ...
      }


  ## Options

  - `keys` (default: `true`) - if set to false, returns only the values without keys.
  """
  @spec stream(shape(), opts :: enum_opts()) :: Enumerable.t({key(), value()} | value())
  def stream(shape, opts \\ []) do
    table = whereis!(shape)

    match_spec =
      if Keyword.get(opts, :keys, true), do: :"$1", else: {:_, :"$1"}

    Stream.resource(
      fn -> nil end,
      fn
        nil ->
          case :ets.match(table, match_spec, 10) do
            {match, continuation} -> {List.flatten(match), continuation}
            :"$end_of_table" -> {:halt, nil}
          end

        :"$end_of_table" ->
          {:halt, nil}

        continuation ->
          case :ets.match(continuation) do
            {match, continuation} -> {List.flatten(match), continuation}
            :"$end_of_table" -> {:halt, nil}
          end
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Get all data in the shape as a map.

  The keys of the map will be the sync stream keys.

      {:ok, _pid} = Phoenix.Sync.Shape.start_link(MyApp.Todo, name: :todos)

      Phoenix.Sync.Shape.to_map(:todos)
      %{
        ~s|"public"."todos"/"1"| => %MyApp.Todo{id: 1},
        ~s|"public"."todos"/"2"| => %MyApp.Todo{id: 2},
        # ...
      }
  """
  @spec to_map(shape()) :: map()
  def to_map(shape) do
    to_map(shape, & &1)
  end

  @doc """
  Get all data in the shape as a map, transforming the keys and values.

      {:ok, _pid} = Phoenix.Sync.Shape.start_link(MyApp.Todo, name: :todos)

      Phoenix.Sync.Shape.to_map(:todos, fn {_key, %MyApp.Todo{id: id, title: title} ->
        {id, title}
      end)
      %{
        1 => "my first todo",
        2 => "my second todo",
        2 => "my third todo",
      }
  """
  @spec to_map(shape(), ({key(), value()} -> {Map.key(), Map.value()})) :: map()
  def to_map(shape, transform) do
    shape
    |> to_list()
    |> Map.new(transform)
  end

  @doc """
  A wrapper around `Enum.find/3` that searches for a value in the shape.

  The `matcher` function is only passed the shape value, not the key.
  """
  @spec find(shape(), Enum.default(), (value() -> any())) :: value() | Enum.default()
  def find(shape, default \\ nil, matcher) do
    shape
    |> stream(keys: false)
    |> Enum.find(default, matcher)
  end

  defp whereis!(shape) do
    case Shape.Registry.whereis(shape) do
      {:ok, table} -> table
      :error -> raise ArgumentError, "Shape #{inspect(shape)} is not registered"
    end
  end

  @impl GenServer
  def init({stream_args, _shape_opts}) do
    {:ok, %{stream_pid: nil, table: nil, subscriptions: %{}},
     {:continue, {:start_shape, stream_args}}}
  end

  @impl GenServer
  def handle_continue({:start_shape, stream_args}, state) do
    {:ok, table_name} = Shape.Registry.register(self())

    table =
      :ets.new(table_name, [:named_table, :ordered_set, :protected, read_concurrency: true])

    pid = self()

    stream =
      try do
        apply(Phoenix.Sync.Client, :stream, stream_args)
      rescue
        Phoenix.Sync.Sandbox.Error ->
          # TODO: some way of lazily subscribing to the stream once a shared-mode sandbox
          # is available.
          # Emit a single up-to-date message to indicate that the shape is ready
          # followed by an infinite but empty stream.
          Stream.concat(
            [[%Message.ControlMessage{control: :up_to_date}]],
            Stream.repeatedly(fn -> [] end)
          )
          |> Stream.flat_map(& &1)
      end

    {:ok, pid} =
      Task.start_link(fn ->
        stream |> Stream.each(&send(pid, {:sync_message, &1})) |> Stream.run()
      end)

    {:noreply, %{state | stream_pid: pid, table: table}}
  end

  @impl GenServer
  def handle_call(:subscribers, _from, state) do
    {:reply, Map.keys(state.subscriptions), state}
  end

  def handle_call({:subscribe, opts}, {pid, _}, state) do
    ref = Process.monitor(pid)
    filter = build_subscription_filter(opts)
    tag = Keyword.get(opts, :tag, :sync)

    state = Map.update!(state, :subscriptions, &Map.put(&1, pid, {filter, tag, ref}))

    {:reply, ref, state}
  end

  def handle_call(:unsubscribe, {pid, _}, state) do
    subscriptions = Map.delete(state.subscriptions, pid)
    {:reply, :ok, %{state | subscriptions: subscriptions}}
  end

  @impl GenServer
  def handle_info({:sync_message, msg}, state) do
    state = state |> handle_sync_message(msg) |> notify_subscribers(msg)

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    subscriptions = Map.delete(state.subscriptions, pid)
    {:noreply, %{state | subscriptions: subscriptions}}
  end

  defp notify_subscribers(state, msg) do
    event = msg_event(msg)

    Enum.each(state.subscriptions, fn {pid, {filter, tag, ref}} ->
      if filter.(msg), do: send(pid, {tag, ref, event})
    end)

    state
  end

  defp msg_event(%Message.ChangeMessage{key: key, headers: %{operation: operation}} = msg) do
    {operation, {key, msg.value}}
  end

  defp msg_event(%Message.ControlMessage{control: control}) do
    control
  end

  defp handle_sync_message(state, %Message.ChangeMessage{headers: %{operation: operation}} = msg) do
    case operation do
      :delete ->
        :ets.delete(state.table, msg.key)

      op when op in [:insert, :update] ->
        :ets.insert(state.table, {msg.key, msg.value})
    end

    state
  end

  defp handle_sync_message(state, %Message.ControlMessage{control: :up_to_date}) do
    state
  end

  defp handle_sync_message(state, %Message.ControlMessage{control: :must_refetch}) do
    :ets.delete_all_objects(state.table)
    state
  end

  defp build_subscription_filter(opts) do
    case Keyword.fetch(opts, :only) do
      {:ok, only} ->
        types = List.wrap(only)

        types =
          types
          |> Enum.flat_map(fn
            :changes -> [:insert, :update, :delete]
            type -> [type]
          end)
          |> Enum.uniq()

        fn msg -> Enum.any?(types, &message_matches_type(&1, msg)) end

      :error ->
        fn _ -> true end
    end
  end

  defp message_matches_type(operation, %Message.ChangeMessage{headers: %{operation: operation}}),
    do: true

  defp message_matches_type(control, %Message.ControlMessage{control: control}), do: true
  defp message_matches_type(_, _), do: false

  # shape may be of form [SchemaModule, where: "..."] which is fine syntactically but breaks Keyword functions
  defp validate_args(args) when is_list(args) do
    case Enum.split_with(args, &is_tuple/1) do
      {opts, []} ->
        {stream_opts, shape_opts} = extract_shape_opts(opts)
        {:ok, [shape_opts], stream_opts}

      {opts, [queryable]} ->
        if PredefinedShape.is_queryable?(queryable) do
          {stream_opts, shape_opts} = extract_shape_opts(opts)
          {:ok, [queryable, shape_opts], stream_opts}
        else
          {:error, "#{queryable} is not a valid Ecto query"}
        end

      {_opts, [_s1, _s2 | _] = queryables} ->
        {:error, "Multiple queryables provided: #{inspect(queryables)}"}
    end
  end

  defp validate_args(queryable) do
    if PredefinedShape.is_queryable?(queryable) do
      {:ok, [queryable, []], []}
    else
      {:error, "#{queryable} is not a valid Ecto query"}
    end
  end

  defp extract_shape_opts(args) when is_list(args) do
    Keyword.split(args, [:name])
  end
end
