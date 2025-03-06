defmodule Phoenix.Sync.Application do
  use Application

  require Logger

  @default_adapter Phoenix.Sync.Electric

  @impl true
  def start(_type, _args) do
    case children() |> dbg do
      {:ok, children} ->
        Supervisor.start_link(children, strategy: :one_for_one, name: Phoenix.Sync.Supervisor)

      {:error, reason} ->
        Logger.warning(reason)
        Supervisor.start_link([], strategy: :one_for_one, name: Phoenix.Sync.Supervisor)
    end
  end

  @doc false
  def config do
    Application.get_all_env(:phoenix_sync)
  end

  @doc false
  def adapter do
    config() |> adapter()
  end

  @doc false
  def adapter(opts) do
    Keyword.get(opts, :adapter, @default_adapter)
  end

  @doc false
  def children do
    config() |> children()
  end

  @doc false
  def children(opts) when is_list(opts) do
    {adapter, env} = adapter_env(opts)

    apply(adapter, :children, [env, opts])
  end

  @doc false
  def adapter_env(opts) do
    {
      adapter(),
      Keyword.get(opts, :env, :prod)
    }
  end

  @doc """
  Returns the required adapter configuration for your Phoenix Endpoint or
  `Plug.Router`.

  ## Phoenix

  Configure your endpoint with the configuration at runtime by passing the
  `phoenix_sync` configuration to your endpoint in the `Application.start/2`
  callback:

      def start(_type, _args) do
        children = [
          # ...
          {MyAppWeb.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
        ]
      end

  ## Plug

  Add the configuration to the Plug opts in your server configuration:

      children = [
        {Bandit, plug: {MyApp.Router, phoenix_sync: Phoenix.Sync.plug_opts()}}
      ]

  Your `Plug.Router` must be configured with
  [`copy_opts_to_assign`](https://hexdocs.pm/plug/Plug.Builder.html#module-options) and you should `use` the rele

      defmodule MyApp.Router do
        use Plug.Router, copy_opts_to_assign: :options

        use Phoenix.Sync.Controller
        use Phoenix.Sync.Router

        plug :match
        plug :dispatch

        sync "/shapes/todos", Todos.Todo

        get "/shapes/user-todos" do
          %{"user_id" => user_id} = conn.params
          sync_render(conn, from(t in Todos.Todo, where: t.owner_id == ^user_id)
        end
      end
  """
  @spec plug_opts() :: keyword()
  def plug_opts do
    config() |> plug_opts()
  end

  @doc false
  def plug_opts(opts) when is_list(opts) do
    {adapter, env} = adapter_env(opts)

    apply(adapter, :plug_opts, [env, opts]) |> dbg
  end

  @doc false
  def fetch_with_error(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, url} -> {:ok, url}
      :error -> {:error, "Missing required key #{inspect(key)}"}
    end
  end
end
