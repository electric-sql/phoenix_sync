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

      # persistent_kv = Electric.PersistentKV.Memory.new!()
      inspector = {Sandbox.Inspector, stack_id}

      %{pid: pool} = Ecto.Adapter.lookup_meta(repo.get_dynamic_repo())

      registry = :"#{__MODULE__}.Registry-#{stack_id}"

      [
        purge_all_shapes?: false,
        stack_id: stack_id,
        storage: {
          Electric.ShapeCache.InMemoryStorage,
          %{stack_id: stack_id, table_base_name: :"#{stack_id}"}
        },
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
      # shape_cache_spec = {Electric.ShapeCache, shape_cache_opts}

      config = config(stack_id, repo, owner)
      shape_cache_spec = {Electric.ShapeCache, config}
      persistent_kv = Electric.PersistentKV.Memory.new!()

      shape_status_spec =
        {Electric.ShapeCache.ShapeStatus,
         %Electric.ShapeCache.ShapeStatus{
           shape_meta_table: shape_meta_table(stack_id)
         }}

      children = [
        {Registry, keys: :duplicate, name: config[:registry]},
        {Electric.ProcessRegistry, stack_id: stack_id},
        {Electric.StatusMonitor, stack_id},
        {Electric.Shapes.Monitor,
         stack_id: stack_id,
         storage: config[:storage],
         shape_status: shape_status_spec,
         publication_manager: config[:publication_manager]},
        # TODO: start an electric stack, decoupled from the db connection
        #       with in memory storage, a mock publication_manager and inspector
        Supervisor.child_spec(
          {
            Electric.Replication.Supervisor,
            stack_id: stack_id,
            shape_cache: shape_cache_spec,
            publication_manager: config[:publication_manager],
            log_collector: {
              Electric.Replication.ShapeLogCollector,
              stack_id: stack_id, inspector: config[:inspector], persistent_kv: persistent_kv
            },
            schema_reconciler: {Phoenix.Sync.Sandbox.SchemaReconciler, stack_id},
            stack_events_registry: config[:registry]
          },
          restart: :temporary
        ),
        {Sandbox.Inspector, stack_id: stack_id, repo: repo},
        {Sandbox.Producer, stack_id: stack_id},
        {DynamicSupervisor,
         name: Phoenix.Sync.Sandbox.Fetch.name(stack_id), strategy: :one_for_one}
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end

    Code.ensure_loaded(Electric.ShapeCache)

    if function_exported?(Electric.ShapeCache, :get_shape_meta_table, 1) do
      defp shape_meta_table(stack_id) do
        Electric.ShapeCache.get_shape_meta_table(stack_id: stack_id)
      end
    else
      defp shape_meta_table(stack_id) do
        Electric.ShapeCache.ShapeStatus.shape_meta_table(stack_id)
      end
    end
  end
end
