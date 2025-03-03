defmodule Support.ElectricHelpers do
  alias Electric.Postgres.Inspector.EtsInspector
  alias Electric.Shapes.Api

  import ExUnit.Callbacks

  @endpoint Phoenix.Sync.LiveViewTest.Endpoint

  defmacro __using__(opts) do
    endpoint_module = opts[:endpoint] || @endpoint

    start_endpoint =
      if endpoint_module != @endpoint do
        quote do
          setup_all do
            ExUnit.CaptureLog.capture_log(fn -> start_supervised!(unquote(endpoint_module)) end)
            :ok
          end
        end
      end

    quote do
      import Support.ElectricHelpers
      import Support.DbSetup

      @endpoint unquote(endpoint_module)

      unquote(start_endpoint)

      defp define_endpoint(_ctx) do
        endpoint =
          @endpoint ||
            raise "Set @endpoint before calling define_endpoint"

        [endpoint: endpoint]
      end
    end
  end

  def endpoint, do: @endpoint

  def with_stack_id_from_test(ctx) do
    stack_id = full_test_name(ctx)
    registry_name = Electric.ProcessRegistry.registry_name(stack_id)

    [stack_id: stack_id, process_registry: registry_name]
  end

  def wait_for_stack(%{stack_id: stack_id}, fun) do
    wait_for_stack(stack_id, fun)
  end

  def wait_for_stack(stack_id, fun) do
    ref = Electric.StackSupervisor.subscribe_to_stack_events(stack_id)

    result = fun.()

    receive do
      {:stack_status, ^ref, :ready} -> result
    after
      2000 -> raise "Stack not ready"
    end
  end

  def with_stack_config(%{stack_id: stack_id} = ctx) do
    kv = %Electric.PersistentKV.Memory{
      parent: self(),
      pid: ExUnit.Callbacks.start_supervised!(Electric.PersistentKV.Memory, restart: :temporary)
    }

    storage =
      Electric.ShapeCache.Storage.shared_opts(
        {Electric.ShapeCache.InMemoryStorage,
         stack_id: stack_id, table_base_name: :"in_memory_storage_#{stack_id}"}
      )

    publication_name = "electric_test_pub_#{:erlang.phash2(stack_id)}"

    inspector = {EtsInspector, stack_id: stack_id, server: EtsInspector.name(stack_id: stack_id)}

    shape_cache = {Electric.ShapeCache, [stack_id: stack_id]}

    config = %{
      stack_id: stack_id,
      registry: Electric.StackSupervisor.registry_name(stack_id),
      shape_cache: shape_cache,
      persistent_kv: kv,
      storage: storage,
      stack_events_registry: Electric.stack_events_registry(),
      inspector: inspector,
      publication_name: publication_name,
      slot_name: "electric_test_slot_#{:erlang.phash2(stack_id)}",
      connection_opts: ctx.db_config,
      replication_slot_temporary?: true
    }

    Map.put(config, :stack_config, config)
  end

  def with_stack(%{stack_id: stack_id} = ctx) do
    ctx_config =
      %{stack_config: config} =
      case ctx do
        %{stack_config: stack_config} -> stack_config
        ctx -> with_stack_config(ctx)
      end

    stack_supervisor =
      wait_for_stack(stack_id, fn ->
        ExUnit.Callbacks.start_link_supervised!(
          {Electric.StackSupervisor,
           stack_id: stack_id,
           persistent_kv: config.persistent_kv,
           storage: config.storage,
           connection_opts: config.connection_opts,
           stack_events_registry: Electric.stack_events_registry(),
           replication_opts: [
             slot_name: config.slot_name,
             publication_name: config.publication_name,
             try_creating_publication?: true,
             slot_temporary?: config.replication_slot_temporary?
           ],
           pool_opts: [
             backoff_type: :stop,
             max_restarts: 0,
             pool_size: 2
           ]}
        )
      end)

    Map.put(ctx_config, :stack_supervisor, stack_supervisor)
  end

  def start_embedded(%{stack_config: stack_config, sync_config: sync_config} = ctx) do
    config =
      Keyword.update!(sync_config, :electric, &Keyword.merge(&1, Enum.to_list(stack_config)))

    {:ok, children} = Phoenix.Sync.Application.children(config)

    wait_for_stack(ctx, fn ->
      for child <- children do
        start_link_supervised!(child)
      end
    end)

    [electric_opts: config]
  end

  def configure_endpoint(%{electric_opts: electric_opts} = ctx) do
    endpoint = ctx.endpoint || @endpoint

    if endpoint == @endpoint && ctx.async,
      do: raise(RuntimeError, message: "do not use configure_endpoint in async tests")

    Phoenix.Config.put(endpoint, :electric, Phoenix.Sync.Application.plug_opts(electric_opts))

    [endpoint: endpoint]
  end

  defp full_test_name(ctx) do
    "#{ctx.module} #{ctx.test}"
  end

  def electric_opts(ctx) do
    [
      stack_id: ctx.stack_id,
      provided_database_id: ctx.stack_id,
      pg_id: nil,
      stack_events_registry: ctx.stack_events_registry,
      shape_cache: ctx.shape_cache,
      storage: ctx.storage,
      inspector: ctx.inspector,
      persistent_kv: ctx.persistent_kv,
      registry: ctx.registry,
      stack_ready_timeout: Access.get(ctx, :stack_ready_timeout, 100),
      long_poll_timeout: long_poll_timeout(ctx),
      max_age: max_age(ctx),
      stale_age: stale_age(ctx),
      allow_shape_deletion: allow_shape_deletion(ctx)
    ]
  end

  def with_api_server(ctx) do
    port = 33000

    # Phoenix.Sync.plug_config(
    electric_opts = Api.plug_opts(electric_opts(ctx))

    ExUnit.Callbacks.start_link_supervised!(
      {Bandit, plug: {MyEnv.TestRouter, electric: electric_opts}, port: port}
    )

    [port: port]
  end

  defp max_age(ctx), do: Access.get(ctx, :max_age, 60)
  defp stale_age(ctx), do: Access.get(ctx, :stale_age, 300)
  defp long_poll_timeout(ctx), do: Access.get(ctx, :long_poll_timeout, 20_000)
  defp allow_shape_deletion(ctx), do: Access.get(ctx, :allow_shape_deletion, false)
end
