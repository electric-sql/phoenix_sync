defmodule Mix.Tasks.PhoenixSync.Install.Docs do
  @moduledoc false

  @spec short_doc() :: String.t()
  def short_doc do
    "Install Phoenix.Sync into an existing Phoenix or Plug application"
  end

  @spec example() :: String.t()
  def example do
    "mix phoenix_sync.install --sync-mode embedded"
  end

  @spec long_doc() :: String.t()
  def long_doc do
    """
    #{short_doc()}

    Usually invoked using `igniter.install`:

    ```sh
    mix igniter.install phoenix_sync --sync-mode embedded
    ```

    But can be invoked directly if `:phoenix_sync` is already a dependency:

    ```sh
    #{example()}
    ```

    ## Options

    * `--sync-mode` - How to connect to Electric, either `embedded` or `http`.

      - `embedded` - `:electric` will be added as a dependency and will connect to the database your repo is configured for.
      - `http` - You'll need to specify the `--sync-url` to the remote Electric server.

    ### Options for `embedded` mode

    * `--no-sync-sandbox` - Disable the test sandbox

    ### Options for `http` mode

    * `--sync-url` (required) - The URL of the Electric server, required for `http` mode.
    """
  end
end

if Code.ensure_loaded?(Igniter) do
  defmodule Mix.Tasks.PhoenixSync.Install do
    @shortdoc "#{__MODULE__.Docs.short_doc()}"

    @moduledoc __MODULE__.Docs.long_doc()

    use Igniter.Mix.Task

    require Igniter.Code.Function

    @impl Igniter.Mix.Task
    def info(_argv, _composing_task) do
      %Igniter.Mix.Task.Info{
        group: :phoenix_sync,
        adds_deps: [],
        installs: [],
        example: __MODULE__.Docs.example(),
        only: nil,
        positional: [],
        composes: [],
        schema: [
          sync_mode: :string,
          sync_url: :string,
          sync_sandbox: :boolean
        ],
        defaults: [],
        aliases: [],
        required: [:sync_mode]
      }
    end

    @valid_modes ~w[embedded http]

    @impl Igniter.Mix.Task
    def igniter(igniter) do
      {:ok, mode} = Keyword.fetch(igniter.args.options, :sync_mode)

      if mode not in @valid_modes do
        Igniter.add_issue(
          igniter,
          "mode #{inspect(mode)} is invlid, valid modes are: #{@valid_modes |> Enum.join(", ")}"
        )
      else
        add_dependencies(igniter, mode)
      end
    end

    defp add_dependencies(igniter, "http") do
      case Keyword.fetch(igniter.args.options, :sync_url) do
        {:ok, url} ->
          igniter
          |> base_configuration(:http)
          |> Igniter.Project.Config.configure_new(
            "config.exs",
            :phoenix_sync,
            [:url],
            url
          )
          |> Igniter.Project.Config.configure_new(
            "config.exs",
            :phoenix_sync,
            [:credentials],
            secret: "MY_SECRET",
            source_id: "00000000-0000-0000-0000-000000000000"
          )
          |> configure_endpoint()

        :error ->
          Igniter.add_issue(igniter, "`--sync-url` is required for :http mode")
      end
    end

    defp add_dependencies(igniter, "embedded") do
      igniter
      |> Igniter.Project.Deps.add_dep({:electric, required_electric_version()}, error?: true)
      |> base_configuration(:embedded)
      |> find_repo()
      |> configure_repo()
      |> configure_endpoint()
    end

    defp configure_endpoint(igniter) do
      application = Igniter.Project.Application.app_module(igniter)

      case Igniter.Libs.Phoenix.select_endpoint(igniter) do
        {igniter, nil} ->
          configure_plug_app(igniter, application)

        {igniter, endpoint} ->
          configure_phoenix_endpoint(igniter, application, endpoint)
      end
    end

    defp configure_plug_app(igniter, application) do
      # find plug module in application children and add config there
      set_plug_opts(igniter, application, [Plug.Cowboy, Bandit], fn zipper ->
        with {:ok, zipper} <- Igniter.Code.Tuple.tuple_elem(zipper, 1) do
          Igniter.Code.Keyword.set_keyword_key(
            zipper,
            :plug,
            nil,
            fn zipper ->
              if Igniter.Code.Tuple.tuple?(zipper) do
                with {:ok, zipper} <- Igniter.Code.Tuple.tuple_elem(zipper, 1) do
                  Igniter.Code.Keyword.set_keyword_key(
                    zipper,
                    :phoenix_sync,
                    quote(do: Phoenix.Sync.plug_opts())
                  )
                end
              else
                with {:ok, plug} <- Igniter.Code.Common.expand_literal(zipper) do
                  {:ok,
                   zipper
                   |> Sourceror.Zipper.search_pattern("#{inspect(plug)}")
                   |> Igniter.Code.Common.replace_code(
                     "{#{inspect(plug)}, phoenix_sync: Phoenix.Sync.plug_opts()}"
                   )}
                end
              end
            end
          )
        end
      end)
    end

    defp configure_phoenix_endpoint(igniter, application, endpoint) do
      set_plug_opts(igniter, application, [endpoint], fn zipper ->
        # gets called with a zipper on the endpoint module in the list of children

        if Igniter.Code.Tuple.tuple?(zipper) do
          with {:ok, zipper} <- Igniter.Code.Tuple.tuple_elem(zipper, 1) do
            Igniter.Code.Keyword.set_keyword_key(
              zipper,
              :phoenix_sync,
              quote(do: Phoenix.Sync.plug_opts())
            )
          end
        else
          # the search pattern call results in replacing the module name with
          # the configuration whilst preserving the preceding comments
          {:ok,
           zipper
           |> Sourceror.Zipper.search_pattern("#{inspect(endpoint)}")
           |> Igniter.Code.Common.replace_code(
             "{#{inspect(endpoint)}, phoenix_sync: Phoenix.Sync.plug_opts()}"
           )}
        end
      end)
    end

    defp configure_repo(%{issues: [_ | _] = _issues} = igniter) do
      igniter
    end

    defp configure_repo(igniter) do
      case igniter.assigns do
        %{repo: repo} when is_atom(repo) ->
          igniter =
            igniter
            |> Igniter.Project.Config.configure_new("config.exs", :phoenix_sync, [:repo], repo)
            |> Igniter.Project.Config.configure_new("test.exs", :phoenix_sync, [:mode], :sandbox)
            |> Igniter.Project.Config.configure_new(
              "test.exs",
              :phoenix_sync,
              [:env],
              {:code, quote(do: config_env())}
            )

          enable_sandbox? = Keyword.get(igniter.args.options, :sync_sandbox, true)

          if enable_sandbox? do
            igniter
            |> Igniter.Project.Module.find_and_update_module!(
              repo,
              fn zipper ->
                with {:ok, zipper} <-
                       Igniter.Code.Function.move_to_function_call(zipper, :use, [2]),
                     {:ok, zipper} <-
                       Igniter.Code.Function.update_nth_argument(zipper, 1, fn zipper ->
                         adapter = quote(do: Phoenix.Sync.Sandbox.Postgres.adapter())

                         Igniter.Code.Keyword.set_keyword_key(
                           zipper,
                           :adapter,
                           adapter,
                           fn z -> {:ok, Igniter.Code.Common.replace_code(z, adapter)} end
                         )
                       end),
                     {:ok, zipper} <- Igniter.Code.Module.move_to_use(zipper, Ecto.Repo),
                     zipper <-
                       Igniter.Code.Common.add_code(zipper, "use Phoenix.Sync.Sandbox.Postgres",
                         placement: :before
                       ) do
                  {:ok, zipper}
                end
              end
            )
          else
            igniter
          end

        _ ->
          igniter
          |> Igniter.add_notice("No Ecto.Repo found, adding example `connection_opts` to config")
          |> Igniter.Project.Config.configure_new(
            "config.exs",
            :phoenix_sync,
            [:connection_opts],
            {:code,
             Sourceror.parse_string!("""
             # add your real database connection details
             [
               username: "your_username",
               password: "your_password",
               hostname: "localhost",
               database: "your_database",
               port: 5432,
               sslmode: :disable
             ]
             """)}
          )
      end
    end

    defp base_configuration(igniter, mode) do
      igniter
      |> Igniter.Project.Config.configure_new(
        "config.exs",
        :phoenix_sync,
        [:mode],
        mode
      )
      |> Igniter.Project.Config.configure_new(
        "config.exs",
        :phoenix_sync,
        [:env],
        {:code, quote(do: config_env())}
      )
    end

    defp required_electric_version do
      Phoenix.Sync.MixProject.project()
      |> Keyword.fetch!(:deps)
      |> Enum.find(&match?({:electric, _, _}, &1))
      |> elem(1)
    end

    defp find_repo(igniter) do
      case Igniter.Libs.Ecto.select_repo(igniter) do
        {igniter, nil} ->
          Igniter.add_notice(
            igniter,
            """
            No Ecto.Repo found in application environment.

            To use `embedded` mode you must add `connection_opts` to your config, e.g.

            config :phoenix_sync,
              env: config_env(),
              mode: :embedded,
              connection_opts: [
                username: "your_username",
                password: "your_password",
                hostname: "localhost",
                database: "your_database",
                port: 5432
              ]
            """
          )

        {igniter, repo} ->
          Igniter.assign(igniter, :repo, repo)
      end
    end

    defp set_plug_opts(igniter, application, modify_modules, updater)
         when is_list(modify_modules) do
      Igniter.Project.Module.find_and_update_module!(igniter, application, fn zipper ->
        with {:ok, zipper} <- Igniter.Code.Function.move_to_def(zipper, :start, 2),
             {:ok, zipper} <-
               Igniter.Code.Function.move_to_function_call_in_current_scope(
                 zipper,
                 :=,
                 [2],
                 fn call ->
                   Igniter.Code.Function.argument_matches_pattern?(
                     call,
                     0,
                     {:children, _, context} when is_atom(context)
                   ) &&
                     Igniter.Code.Function.argument_matches_pattern?(call, 1, v when is_list(v))
                 end
               ),
             {:ok, zipper} <- Igniter.Code.Function.move_to_nth_argument(zipper, 1) do
          case Igniter.Code.List.move_to_list_item(zipper, fn item ->
                 case extract_child_module(item) do
                   {:ok, child_module} ->
                     Enum.any?(modify_modules, fn modify_module ->
                       Igniter.Code.Common.nodes_equal?(child_module, modify_module)
                     end)

                   :error ->
                     false
                 end
               end) do
            {:ok, zipper} ->
              updater.(zipper)

            :error ->
              {:warning,
               """
               Could not find a suitable `children = [...]` assignment in the `start` function of the `#{inspect(application)}` module.
               Please add `phoenix_sync: Phoenix.Sync.plug_opts()` to your Phoenix endpoint or Plug module configuration
               """}
          end
        else
          _ ->
            {:warning,
             """
             Could not find a `children = [...]` assignment in the `start` function of the `#{inspect(application)}` module.
             Please add `phoenix_sync: Phoenix.Sync.plug_opts()` to your Phoenix endpoint or Plug module configuration
             """}
        end
      end)
    end

    defp extract_child_module(zipper) do
      if Igniter.Code.Tuple.tuple?(zipper) do
        with {:ok, elem} <- Igniter.Code.Tuple.tuple_elem(zipper, 0) do
          {:ok, Igniter.Code.Common.expand_alias(elem)}
        end
      else
        {:ok, Igniter.Code.Common.expand_alias(zipper)}
      end
    end
  end
else
  defmodule Mix.Tasks.PhoenixSync.Install do
    @shortdoc "#{__MODULE__.Docs.short_doc()} | Install `igniter` to use"

    @moduledoc __MODULE__.Docs.long_doc()

    use Mix.Task

    @impl Mix.Task
    def run(_argv) do
      Mix.shell().error("""
      The task 'phoenix_sync.install' requires igniter. Please install igniter and try again.

      For more information, see: https://hexdocs.pm/igniter/readme.html#installation
      """)

      exit({:shutdown, 1})
    end
  end
end
