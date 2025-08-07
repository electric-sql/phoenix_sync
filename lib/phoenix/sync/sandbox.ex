if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
  defmodule Phoenix.Sync.Sandbox do
    @moduledoc """
    Integration between `Ecto.Adapters.SQL.Sandbox` and `Electric` that produces
    replication events from Ecto operations within a sandboxed connection.

    In normal operation `Electric` creates and consumes a logical replication
    slot on your Postgres database. This makes testing difficult because this
    replication stream is stateful and will not emit events when testing using
    `Ecto.Adapters.SQL.Sandbox` (which aborts all operations before they could
    appear in the replication stream).

    `Phoenix.Sync.Sandbox` uses a custom `Ecto.Adapter` that intercepts writes
    within a sandboxed connection and emits change events to a per-test
    replication stack.

    ## Integration

    ### Step 1

    In your `config/test.exs` file, set the `mode` to `:sandbox`:

        config :phoenix_sync,
          env: config_env(),
          mode: :sandbox

    ### Step 2

    Replace your `Ecto.Repo`'s adapter with our sandbox adapter macro:


        # before
        defmodule MyApp.Repo do
          use Ecto.Repo,
            otp_app: :my_app,
            adapter: Ecto.Adapters.Postgres
        end


        # after
        defmodule MyApp.Repo do
          use Phoenix.Sync.Sandbox.Postgres

          use Ecto.Repo,
            otp_app: :my_app,
            adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
        end

    This macro will configure the repo with `Ecto.Adapters.Postgres` in `dev`
    and `prod` environments but enable intercepting db writes in tests.

    ### Step 3

    In your test file, after your `Ecto.Adapters.SQL.Sandbox.checkout(Repo)`
    setup call, start a sandbox stack for your repo:


        # before
        setup do
          :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
        end

        # after
        setup do
          :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
          # start our sandbox replication stack
          Phoenix.Sync.Sandbox.start!(Repo)
        end

    Or if you're using `Ecto.Adapters.SQL.Sandbox.start_owner!/2`:

        # before
        setup do
          pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo)
          on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
        end

        # after
        setup do
          pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo)
          on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
          # start our sandbox replication stack
          Phoenix.Sync.Sandbox.start!(Repo, pid)
        end

    Now in your tests, inserting via the configured repo will emit change
    messages for sandboxed writes, exactly as if you were reading from the
    Postgres replication stream.

    ## Collaborating processes

    ### Allowances

    The `Phoenix.Sync.Sandbox` uses the same ownership model as
    `Ecto.Adapters.SQL.Sandbox` -- and processes that automatically inherit
    access to the sandboxed connection will also register themselves to the
    test electric stack.

        test "tasks have access" do
          start_supervised!({Task, fn ->
            # this will succeed and broadcast a change event on the test's
            # sandbox replication stream
            Repo.insert!(%MyApp.Task{title: "Test Task"})
          end})
        end

    However processes started outside of the test process tree that need to be
    [explictly granted access to the sandboxed
    connection](https://hexdocs.pm/ecto_sql/Ecto.Adapters.SQL.Sandbox.html#module-allowances)
    will also need to be explicitly registered to the current test's
    replication stream using `#{inspect(__MODULE__)}.allow/3`. This calls
    `Ecto.Adapters.SQL.Sandbox.allow/3` and also registers the `allow` pid
    against the current process's replication stack.

    So where you would normally need to call
    `Ecto.Adapters.SQL.Sandbox.allow/3` simply call
    `#{inspect(__MODULE__)}.allow/3` instead:

        test "calls worker that runs a query" do
          allow = Process.whereis(MyApp.Worker)
          #{inspect(__MODULE__)}.allow(Repo, self(), allow)
          GenServer.call(MyApp.Worker, :run_query)
        end

    ### Shared mode

    If you're configuring your Ecto sandbox in shared mode, you also need to configure
    the `Phoenix.Sync.Sandbox` to use shared mode by passing `shared: true` to `Phoenix.Sync.Sandbox.start!/3`.

        # before
        setup(tags) do
          pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not tags[:async])
          on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
        end

        # after
        setup(tags) do
          pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not tags[:async])
          on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
          Phoenix.Sync.Sandbox.start!(Repo, pid, shared: not tags[:async])
        end

    ## Integrations

    ### Oban

    Because `Phoenix.Sync.Sandbox.Postgres.adapter/0` configures your
    `Ecto.Repo` to use a non-standard adapter module it causes problems
    with Oban's migration system.

    If you see an error like:

    ``` txt
    ** (KeyError) key :migrator not found in: [
      # your repo config...
    ]
    ```

    then the solution is to force Oban to use its Postgres migrator in your `config/test.exs`:

        config :my_app, MyApp.Repo,
          # base config...
          migrator: Oban.Migrations.Postgres

    ## Limitations

    The sandbox adapter will intercept the following functions:

    - `c:Ecto.Repo.delete_all/2`
    - `c:Ecto.Repo.update_all/3`
    - `c:Ecto.Repo.delete/2`
    - `c:Ecto.Repo.delete!/2`
    - `c:Ecto.Repo.insert/2`
    - `c:Ecto.Repo.insert!/2`
    - `c:Ecto.Repo.insert_all/3`
    - `c:Ecto.Repo.update/2`
    - `c:Ecto.Repo.update!/2`

    It does this by potentially re-writing the SQL for the query. Because of this
    there will be some more complex queries that will fail. Please [raise an
    issue](https://github.com/electric-sql/phoenix_sync/issues/new)
    if you hit problems.
    """

    @doc false
    use Supervisor

    alias __MODULE__

    @type start_opts() :: [{:shared, boolean()}]

    @registry __MODULE__.Registry

    @doc false
    def start_link(_args \\ []) do
      Supervisor.start_link(__MODULE__, [], name: __MODULE__)
    end

    @doc """
    See `start!/3`.
    """
    @spec start!(Ecto.Repo.t()) :: :ok | no_return()
    def start!(repo) when is_atom(repo) do
      start!(repo, self(), [])
    end

    @doc """
    See `start!/3`.
    """
    @spec start!(Ecto.Repo.t(), pid() | start_opts()) :: :ok | no_return()
    def start!(repo, opts_or_owner)

    def start!(repo, opts) when is_atom(repo) and is_list(opts) do
      start!(repo, self(), opts)
    end

    def start!(repo, owner) when is_atom(repo) and is_pid(owner) do
      start!(repo, owner, [])
    end

    @doc """
    Start a sandbox instance for `repo` linked to the given `owner` process.

    Call this after your `Ecto.Adapters.SQL.Sandbox.start_owner!/2` or
    `Ecto.Adapters.SQL.Sandbox.checkout/2` call.

        setup do
          :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
          Phoenix.Sync.Sandbox.start!(Repo)
        end

    ### Options

    - `shared` (default: `false`) - start the sandbox in shared mode. Only
      enable shared mode if your repo sandbox is also in shared mode.
    - `tags` - pass the test tags in order to generate a stack identifier based
      on the current test context
    """
    @spec start!(Ecto.Repo.t(), pid(), start_opts()) :: :ok | no_return()
    def start!(repo, owner, opts) when is_atom(repo) and is_pid(owner) and is_list(opts) do
      test_pid = self()
      validate_sandbox_repo!(repo)

      case stack_id() do
        nil ->
          stack_id = generate_stack_id(opts)

          {:ok, _pid} =
            ExUnit.Callbacks.start_supervised(
              {__MODULE__.Stack, stack_id: stack_id, repo: repo, owner: owner}
            )

          :ok = maybe_set_shared_mode(owner, stack_id, opts)

          # give the inspector access to the sandboxed connection
          Ecto.Adapters.SQL.Sandbox.allow(repo, owner, Sandbox.Inspector.name(stack_id))

          # mark the stack as ready
          Electric.StatusMonitor.mark_pg_lock_acquired(stack_id, owner)
          Electric.StatusMonitor.mark_replication_client_ready(stack_id, owner)
          Electric.StatusMonitor.mark_connection_pool_ready(stack_id, owner)

          api_config = Sandbox.Stack.config(stack_id, repo)
          api = Electric.Application.api(api_config)

          {:ok, client} =
            Phoenix.Sync.Electric.client(:test, Keyword.put(api_config, :mode, :embedded))

          client = Map.put(client, :pool, {Phoenix.Sync.Sandbox.Fetch, stack_id: stack_id})

          # we link the sandbox to the current (test) process not the connection
          # owner because that's the ownership route that works. The owner
          # is a convenience to link the repo connection to a process who's lifetime
          # is explictly managed rather than a mechanism for linking the test
          # to the sandbox txn.
          Sandbox.StackRegistry.register(test_pid, stack_id)

          # register the electric api and configured client to the stack
          Sandbox.StackRegistry.configure(stack_id, api, client)

          :ok

        stack_id ->
          case GenServer.whereis(__MODULE__.Stack.name(stack_id)) do
            nil -> raise RuntimeError, message: "no stack found for #{inspect(stack_id)}"
            _pid -> :ok
          end
      end
    end

    defp generate_stack_id(opts) do
      tags = Keyword.get(opts, :tags, %{})
      # with parameterised tests the same file:line can be running simultaneously
      uid = System.unique_integer() |> to_string()

      suffix =
        case Map.fetch(tags, :line) do
          {:ok, line} -> ":#{line}"
          :error -> ""
        end

      prefix =
        case Map.fetch(tags, :file) do
          {:ok, file} -> "-" <> Path.relative_to(file, Path.dirname(Mix.Project.project_file()))
          :error -> ""
        end

      "#{inspect(__MODULE__.Stack)}#{uid}#{prefix}#{suffix}"
    end

    defp maybe_set_shared_mode(owner, stack_id, opts) do
      if opts[:shared] do
        Sandbox.StackRegistry.shared_mode(owner, stack_id)
      else
        :ok
      end
    end

    @doc false
    def name(id) do
      {:via, Registry, {@registry, id}}
    end

    @doc false
    def retrieve_api!() do
      stack_id = stack_id!()
      {:ok, api} = Sandbox.StackRegistry.get_api(stack_id)
      api
    end

    @doc false
    def stack_id! do
      stack_id() || raise "No stack_id found. Did you call Phoenix.Sync.Sandbox.start!/1?"
    end

    @doc false
    def stack_id() do
      [self() | Process.get(:"$callers") || []]
      |> lookup_stack_id()
    end

    defp lookup_stack_id(pids) when is_list(pids) do
      # things can be inserted into a repo before the Phoenix.Sync application
      # has even started
      case GenServer.whereis(Sandbox.StackRegistry) do
        nil ->
          raise """
          Phoenix.Sync.Sandbox is not running. Have you set the mode to `:sandbox` in `config/test.exs`?

          # config/test.exs
          config :phoenix_sync,
            env: config_env(),
            mode: :sandbox
          """

        registry_pid when is_pid(registry_pid) ->
          pids
          |> Stream.map(fn pid ->
            Sandbox.StackRegistry.lookup(registry_pid, pid)
          end)
          |> Enum.find(&(!is_nil(&1)))
      end
    end

    @doc """
    Allows the process `name_or_pid` to access the sandboxed transaction.

    This is a wrapper around `Ecto.Adapters.SQL.Sandbox.allow/3` that also
    connects the given process to the active sync instance.

    You should use it instead of `Ecto.Adapters.SQL.Sandbox.allow/3` for tests
    that are using the sandbox replication stack.

    `opts` is passed to `Ecto.Adapters.SQL.Sandbox.allow/3`.
    """
    @spec allow(Ecto.Repo.t(), pid(), pid() | GenServer.name(), keyword()) :: :ok | no_return()
    def allow(repo, parent, name_or_pid, opts \\ []) do
      with :ok <- Ecto.Adapters.SQL.Sandbox.allow(repo, parent, name_or_pid, opts) do
        [parent]
        |> lookup_stack_id()
        |> case do
          nil ->
            raise RuntimeError,
                  "No stack_id found for process #{inspect(parent)}. Did you call Phoenix.Sync.Sandbox.start!/1?"

          stack_id ->
            case GenServer.whereis(name_or_pid) do
              pid when is_pid(pid) ->
                Sandbox.StackRegistry.register(pid, stack_id)

              other ->
                raise "`allow/4` expects a PID or a locally registered process name but lookup returned: #{inspect(other)}"
            end
        end
      end
    end

    @doc false
    @spec init_test_session(Plug.Conn.t(), Ecto.Repo.t(), map()) :: Plug.Conn.t()
    def init_test_session(conn, _repo, session \\ %{}) do
      client = client!()

      conn
      |> Plug.Test.init_test_session(session)
      |> Plug.Conn.put_private(:electric_client, client)
    end

    @doc false
    def plug_opts do
      Phoenix.Sync.Application.plug_opts(mode: :sandbox)
    end

    @doc """
    Retrieve a client configured for the current sandbox stack.

    Example:

        {:ok, client} = Phoenix.Sync.Sandbox.client()
        Electric.Client.stream(client, Todo, replica: :full),

    """
    @spec client() :: {:ok, Electric.Client.t()} | {:error, String.t()}
    def client do
      if stack_id = stack_id() do
        Sandbox.StackRegistry.get_client(stack_id)
      else
        {:error, "No stack_id found. Did you call Phoenix.Sync.Sandbox.start!/1?"}
      end
    end

    @doc """
    As per `client/0` but raises if there is no stack configured for the
    current process.
    """
    @spec client!() :: Electric.Client.t() | no_return()
    def client!() do
      {:ok, client} = Sandbox.StackRegistry.get_client(stack_id!())

      client
    end

    @impl true
    @doc false
    def init(_) do
      children = [
        {Registry, keys: :unique, name: @registry},
        Sandbox.StackRegistry
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end

    defp validate_sandbox_repo!(repo) do
      if Code.ensure_loaded?(repo) && function_exported?(repo, :__adapter__, 0) do
        if repo.__adapter__() != Sandbox.Postgres.Adapter do
          raise RuntimeError,
                "Repo #{inspect(repo)} is not using the Phoenix.Sync.Sandbox.Postgres adapter. Please ensure you have configured your repo to use the sandbox adapter."
        end
      end
    end
  end
end
