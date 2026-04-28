if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.Stack do
    @moduledoc false

    use Supervisor, restart: :transient

    alias Phoenix.Sync.Sandbox

    @json Phoenix.Sync.json_library()

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

    alias Electric.ShapeCache.Storage
    alias Electric.Shapes.Querying

    def snapshot_query(
          task_parent,
          consumer,
          shape_handle,
          shape,
          %{storage: storage, stack_id: stack_id},
          sandbox_pool
        ) do
      Postgrex.transaction(
        sandbox_pool,
        fn conn ->
          xmin = 1000
          xmax = 1100

          send(task_parent, {:ready_to_stream, self(), System.monotonic_time(:millisecond)})

          GenServer.cast(consumer, {:pg_snapshot_known, shape_handle, {xmin, xmax, []}})

          # Enforce display settings *before* querying initial data to maintain consistent
          # formatting between snapshot and live log entries.
          Enum.each(Electric.Postgres.display_settings(), &Postgrex.query!(conn, &1, []))

          chunk_bytes_threshold = Electric.StackConfig.lookup(stack_id, :chunk_bytes_threshold)

          # xmin/xmax/xip_list are uint64, so we need to convert them to strings for JS not to mangle them
          finishing_control_message =
            @json.encode!(%{
              headers: %{
                control: "snapshot-end",
                xmin: to_string(xmin),
                xmax: to_string(xmax),
                xip_list: []
              }
            })

          stream =
            Querying.stream_initial_data(
              conn,
              stack_id,
              shape_handle,
              shape,
              chunk_bytes_threshold
            )
            |> Stream.transform(
              fn -> false end,
              fn item, acc ->
                if not acc do
                  send(task_parent, :data_received)
                  GenServer.cast(consumer, {:snapshot_started, shape_handle})
                end

                {[item], true}
              end,
              fn acc ->
                if not acc do
                  # The stream has been read to the end but we haven't seen a single item in
                  # it. Notify `consumer` anyway since an empty file will have been created by
                  # the storage implementation for the API layer to read the snapshot data
                  # from.
                  send(task_parent, :data_received)
                  GenServer.cast(consumer, {:snapshot_started, shape_handle})
                end

                {[], acc}
              end,
              fn acc ->
                # noop after fun just to be able to specify the last fun which is only
                # available in `Stream.transoform/5`.
                acc
              end
            )
            |> Stream.concat([finishing_control_message])

          Storage.make_new_snapshot!(stream, storage)
        end,
        timeout: :infinity
      )
    end

    defp storage_dir(stack_id) do
      Path.join([System.tmp_dir!(), "#{inspect(__MODULE__)}_#{stack_id}"])
    end

    def config(stack_id, repo, owner \\ nil) do
      publication_manager_spec =
        {Sandbox.PublicationManager, stack_id: stack_id, owner: owner, repo: repo}

      # persistent_kv = Electric.PersistentKV.Memory.new!()
      inspector = {Sandbox.Inspector, stack_id}

      %{pid: pool} = Ecto.Adapter.lookup_meta(repo.get_dynamic_repo())

      registry = Electric.StackSupervisor.registry_name(stack_id)

      storage_dir = storage_dir(stack_id)

      storage = {
        Electric.ShapeCache.PureFileStorage,
        [stack_id: stack_id, storage_dir: storage_dir]
      }

      [
        stack_id: stack_id,
        storage: storage,
        storage_dir: storage_dir,
        inspector: inspector,
        publication_manager: publication_manager_spec,
        chunk_bytes_threshold: 10_485_760,
        db_pool: pool,
        log_producer: Electric.Replication.ShapeLogCollector.name(stack_id),
        consumer_supervisor: Electric.Shapes.DynamicConsumerSupervisor.name(stack_id),
        registry: registry,
        max_shapes: nil
      ]
    end

    def init({stack_id, repo, owner}) do
      config = config(stack_id, repo, owner)
      shape_cache_spec = {Electric.ShapeCache, stack_id: stack_id}
      persistent_kv = Electric.PersistentKV.Memory.new!()

      storage = Storage.shared_opts(config[:storage])

      create_snapshot_fn = fn task_parent, consumer, shape_handle, shape, ctx ->
        snapshot_query(task_parent, consumer, shape_handle, shape, ctx, config[:db_pool])
      end

      children = [
        {Sandbox.Cleanup, {stack_id, config[:storage_dir]}},
        {Electric.ProcessRegistry, partitions: 1, stack_id: stack_id},
        {Registry, keys: :duplicate, name: config[:registry]},
        {Electric.StackConfig,
         stack_id: stack_id,
         seed_config: [
           inspector: config[:inspector],
           create_snapshot_fn: create_snapshot_fn
         ]},
        {Electric.AsyncDeleter,
         stack_id: stack_id,
         storage_dir: config[:storage_dir],
         cleanup_interval_ms: :timer.seconds(60)},
        Storage.stack_child_spec(storage),
        {
          Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
          shape_db_opts: [storage_dir: ":memory:", exclusive_mode: true], stack_id: stack_id
        },
        {Electric.ShapeCache.ShapeStatusOwner, stack_id: stack_id},
        {Electric.StatusMonitor, stack_id: stack_id},
        {Electric.ShapeCache.ShapeCleaner.CleanupTaskSupervisor, stack_id: stack_id},
        {Sandbox.Inspector, stack_id: stack_id, repo: repo, owner: owner},
        {DynamicSupervisor,
         name: Phoenix.Sync.Sandbox.Fetch.name(stack_id), strategy: :one_for_one},
        Supervisor.child_spec(
          {
            Electric.Shapes.Supervisor,
            stack_id: stack_id,
            consumer_supervisor: {Electric.Shapes.DynamicConsumerSupervisor, stack_id: stack_id},
            shape_cleaner:
              {Electric.ShapeCache.ShapeCleaner.CleanupTaskSupervisor, stack_id: stack_id},
            shape_cache: shape_cache_spec,
            publication_manager: config[:publication_manager],
            log_collector: {
              Electric.Replication.ShapeLogCollector.Supervisor,
              stack_id: stack_id, inspector: config[:inspector], persistent_kv: persistent_kv
            },
            schema_reconciler: {Phoenix.Sync.Sandbox.SchemaReconciler, stack_id},
            expiry_manager: {Phoenix.Sync.Sandbox.ExpiryManager, stack_id: stack_id}
          },
          restart: :temporary
        ),
        {Sandbox.Producer, stack_id: stack_id},
        {Sandbox.InitializeStack, stack_id: stack_id}
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end
end
