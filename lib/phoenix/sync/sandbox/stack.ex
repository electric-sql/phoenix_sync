if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.Stack do
    @moduledoc false

    use Supervisor, restart: :transient

    alias Phoenix.Sync.Sandbox

    def child_spec(opts) do
      {:ok, stack_id} = Keyword.fetch(opts, :stack_id)
      {:ok, repo} = Keyword.fetch(opts, :repo)
      {:ok, owner} = Keyword.fetch(opts, :owner)

      %{
        id: {__MODULE__, stack_id},
        start: {__MODULE__, :start_link, [stack_id, repo, owner]},
        type: :supervisor,
        restart: :transient
      }
    end

    def name(stack_id) do
      Phoenix.Sync.Sandbox.name({__MODULE__, stack_id})
    end

    def start_link(stack_id, repo, owner) do
      Supervisor.start_link(__MODULE__, {stack_id, repo, owner}, name: name(stack_id))
    end

    alias Electric.Shapes.Querying
    alias Electric.ShapeCache.Storage

    def snapshot_query(
          parent,
          shape_handle,
          shape,
          db_pool,
          storage,
          stack_id,
          chunk_bytes_threshold
        ) do
      Postgrex.transaction(
        db_pool,
        fn conn ->
          GenServer.cast(parent, {:pg_snapshot_known, shape_handle, {1000, 1100, []}})

          # Enforce display settings *before* querying initial data to maintain consistent
          # formatting between snapshot and live log entries.
          Enum.each(Electric.Postgres.display_settings(), &Postgrex.query!(conn, &1, []))

          stream =
            Querying.stream_initial_data(conn, stack_id, shape, chunk_bytes_threshold)
            |> Stream.transform(
              fn -> false end,
              fn item, acc ->
                if not acc, do: GenServer.cast(parent, {:snapshot_started, shape_handle})
                {[item], true}
              end,
              fn acc ->
                if not acc, do: GenServer.cast(parent, {:snapshot_started, shape_handle})
                acc
              end
            )

          # could pass the shape and then make_new_snapshot! can pass it to row_to_snapshot_item
          # that way it has the relation, but it is still missing the pk_cols
          Storage.make_new_snapshot!(stream, storage)
        end,
        timeout: :infinity
      )
    end

    def config(stack_id, repo, owner \\ nil) do
      publication_manager_spec =
        {Sandbox.PublicationManager, stack_id: stack_id, owner: owner, repo: repo}

      inspector = {Sandbox.Inspector, stack_id}

      %{pid: pool} = Ecto.Adapter.lookup_meta(repo.get_dynamic_repo())

      registry = :"#{__MODULE__}.Registry-#{stack_id}"

      # Use sandbox-specific storage that skips DB snapshot creation
      # InMemoryStorage can't work in sandbox mode because Postgrex.transaction
      # fails when the connection is already in an Ecto sandbox transaction
      storage = {
        Phoenix.Sync.Sandbox.Storage,
        [stack_id: stack_id, table_base_name: :"#{stack_id}"]
      }

      [
        purge_all_shapes?: false,
        stack_id: stack_id,
        storage: storage,
        inspector: inspector,
        publication_manager: publication_manager_spec,
        chunk_bytes_threshold: 10_485_760,
        db_pool: pool,
        create_snapshot_fn: &snapshot_query/7,
        log_producer: Electric.Replication.ShapeLogCollector.name(stack_id),
        consumer_supervisor: Electric.Shapes.DynamicConsumerSupervisor.name(stack_id),
        registry: registry,
        max_shapes: nil
      ]
    end

    def init({stack_id, repo, owner}) do
      config = config(stack_id, repo, owner)
      persistent_kv = Electric.PersistentKV.Memory.new!()

      # Electric 1.2.x: Convert storage to map format for ShapeStatusOwner and ShapeCache
      # Electric.Application.api needs raw keyword list format, but internals need map format
      compiled_storage = Storage.shared_opts(config[:storage])

      # Electric 1.2.x: Use Electric.Shapes.Supervisor instead of Electric.Replication.Supervisor
      # The shapes supervisor handles shape cache, log collection, and consumer supervision
      # Use real Electric modules that register via ProcessRegistry
      shapes_supervisor_opts = [
        stack_id: stack_id,
        shape_cleaner: {Electric.ShapeCache.ShapeCleaner, stack_id: stack_id},
        log_collector: {
          Electric.Replication.ShapeLogCollector,
          stack_id: stack_id, inspector: config[:inspector], persistent_kv: persistent_kv
        },
        publication_manager: config[:publication_manager],
        consumer_supervisor: {Electric.Shapes.DynamicConsumerSupervisor, [stack_id: stack_id]},
        shape_cache: {
          Electric.ShapeCache,
          stack_id: stack_id,
          storage: compiled_storage,
          inspector: config[:inspector],
          publication_manager: config[:publication_manager],
          chunk_bytes_threshold: config[:chunk_bytes_threshold],
          db_pool: config[:db_pool],
          consumer_supervisor: config[:consumer_supervisor],
          registry: config[:registry]
        },
        expiry_manager: {Electric.ShapeCache.ExpiryManager, stack_id: stack_id},
        schema_reconciler:
          {Electric.Replication.SchemaReconciler,
           stack_id: stack_id, inspector: config[:inspector]}
      ]

      children = [
        {Registry, keys: :duplicate, name: config[:registry]},
        {Electric.ProcessRegistry, stack_id: stack_id},
        {Electric.StatusMonitor, [stack_id: stack_id]},
        # Electric 1.2.x: Shapes.Monitor handles reader registration and cleanup
        {Electric.Shapes.Monitor,
         stack_id: stack_id,
         storage: compiled_storage,
         publication_manager: config[:publication_manager]},
        # ShapeStatusOwner must be started before Shapes.Supervisor to create ETS tables
        {Electric.ShapeCache.ShapeStatusOwner, [stack_id: stack_id, storage: compiled_storage]},
        Supervisor.child_spec(
          {Electric.Shapes.Supervisor, shapes_supervisor_opts},
          restart: :temporary
        ),
        {Sandbox.Inspector, stack_id: stack_id, repo: repo},
        {Sandbox.Producer, stack_id: stack_id},
        {DynamicSupervisor,
         name: Phoenix.Sync.Sandbox.Fetch.name(stack_id), strategy: :one_for_one}
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end
end
