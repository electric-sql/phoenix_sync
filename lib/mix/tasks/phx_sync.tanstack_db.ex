defmodule Mix.Tasks.PhxSync.TanstackDb.Docs do
  @moduledoc false

  @spec short_doc() :: String.t()
  def short_doc do
    "A short description of your task"
  end

  @spec example() :: String.t()
  def example do
    "mix phx_sync.tanstack_db"
  end

  @spec long_doc() :: String.t()
  def long_doc do
    """
    #{short_doc()}

    Longer explanation of your task

    ## Example

    ```sh
    #{example()}
    ```

    ## Options

    * `--example-option` or `-e` - Docs for your option
    """
  end
end

if Code.ensure_loaded?(Igniter) do
  defmodule Mix.Tasks.PhxSync.TanstackDb do
    import Igniter.Project.Application, only: [app_name: 1]

    @shortdoc "#{__MODULE__.Docs.short_doc()}"

    @moduledoc __MODULE__.Docs.long_doc()

    use Igniter.Mix.Task

    @impl Igniter.Mix.Task
    def info(_argv, _composing_task) do
      %Igniter.Mix.Task.Info{
        # Groups allow for overlapping arguments for tasks by the same author
        # See the generators guide for more.
        group: :phoenix_sync,
        # *other* dependencies to add
        # i.e `{:foo, "~> 2.0"}`
        adds_deps: [],
        # *other* dependencies to add and call their associated installers, if they exist
        # i.e `{:foo, "~> 2.0"}`
        installs: [],
        # An example invocation
        example: __MODULE__.Docs.example(),
        # a list of positional arguments, i.e `[:file]`
        positional: [],
        # Other tasks your task composes using `Igniter.compose_task`, passing in the CLI argv
        # This ensures your option schema includes options from nested tasks
        composes: [],
        # `OptionParser` schema
        schema: [sync_pnpm: :boolean],
        # Default values for the options in the `schema`
        defaults: [
          sync_pnpm: true
        ],
        # CLI aliases
        aliases: [],
        # A list of options in the schema that are required
        required: []
      }
    end

    @impl Igniter.Mix.Task
    def igniter(igniter) do
      igniter
      |> configure_package_manager()
      |> install_assets()
      |> configure_watchers()
      |> add_task_aliases()
      |> write_layout()
      |> define_routes()
      |> add_caddy_file()
      |> remove_esbuild()
      |> add_ingest_flow()
      |> run_assets_setup()
    end

    defp add_ingest_flow(igniter) do
      alias Igniter.Libs.Phoenix

      web_module = Phoenix.web_module(igniter)
      {igniter, router} = Igniter.Libs.Phoenix.select_router(igniter)

      igniter
      |> Phoenix.add_scope(
        "/ingest",
        """
        pipe_through :api

        # example router for accepting optimistic writes from the client
        # See: https://tanstack.com/db/latest/docs/overview#making-optimistic-mutations
        # post "/mutations", Controllers.IngestController, :ingest
        """,
        arg2: web_module,
        router: router,
        placement: :after
      )
      # phoenix doesn't generally namespace controllers under Web.Controllers
      # but igniter ignores my path here and puts the final file in the location
      # defined by the module name conventions
      |> Igniter.create_new_file(
        "lib/#{Macro.underscore(web_module)}/controllers/ingest_controller.ex",
        """
        defmodule #{inspect(Module.concat([web_module, Controllers, IngestController]))} do
          use #{web_module}, :controller

          # See https://hexdocs.pm/phoenix_sync/readme.html#write-path-sync

          # alias Phoenix.Sync.Writer

          # def ingest(%{assigns: %{current_user: user}} = conn, %{"mutations" => mutations}) do
          #   {:ok, txid, _changes} =
          #     Writer.new()
          #     |> Writer.allow(
          #       Todos.Todo,
          #       accept: [:insert],
          #       check: &Ingest.check_event(&1, user)
          #     )
          #     |> Writer.apply(mutations, Repo, format: Writer.Format.TanstackDB)
          #
          #   json(conn, %{txid: txid})
          # end
        end
        """
      )
    end

    defp add_caddy_file(igniter) do
      igniter
      |> create_or_replace_file("Caddyfile")
    end

    defp define_routes(igniter) do
      {igniter, router} = Igniter.Libs.Phoenix.select_router(igniter)

      igniter
      |> Igniter.Project.Module.find_and_update_module!(
        router,
        fn zipper ->
          with {:ok, zipper} <-
                 Igniter.Code.Function.move_to_function_call(
                   zipper,
                   :get,
                   3,
                   fn function_call ->
                     Igniter.Code.Function.argument_equals?(function_call, 0, "/") &&
                       Igniter.Code.Function.argument_equals?(function_call, 1, PageController) &&
                       Igniter.Code.Function.argument_equals?(function_call, 2, :home)
                   end
                 ),
               {:ok, zipper} <-
                 Igniter.Code.Function.update_nth_argument(zipper, 0, fn zipper ->
                   {:ok,
                    Igniter.Code.Common.replace_code(
                      zipper,
                      Sourceror.parse_string!(~s|"/*page"|)
                    )}
                 end),
               zipper <-
                 Igniter.Code.Common.add_comment(
                   zipper,
                   "Forward all routes onto the root layout since tanstack router does our routing",
                   []
                 ) do
            {:ok, zipper}
          end
        end
      )
    end

    defp run_assets_setup(igniter) do
      if igniter.assigns[:test_mode?] do
        igniter
      else
        Igniter.add_task(igniter, "assets.setup")
      end
    end

    defp write_layout(igniter) do
      igniter
      |> create_or_replace_file(
        "lib/#{app_name(igniter)}_web/components/layouts/root.html.heex",
        "lib/web/components/layouts/root.html.heex"
      )
    end

    defp remove_esbuild(igniter) do
      igniter
      |> Igniter.add_task("deps.unlock", ["tailwind", "esbuild"])
      |> Igniter.add_task("deps.clean", ["tailwind", "esbuild"])
      |> Igniter.Project.Deps.remove_dep(:esbuild)
      |> Igniter.Project.Deps.remove_dep(:tailwind)
      |> Igniter.Project.Config.remove_application_configuration("config.exs", :esbuild)
      |> Igniter.Project.Config.remove_application_configuration("config.exs", :tailwind)
    end

    defp add_task_aliases(igniter) do
      igniter
      |> set_alias(
        "assets.setup",
        "cmd --cd assets #{package_manager(igniter)} install --ignore-workspace"
      )
      |> set_alias(
        "assets.build",
        "cmd --cd assets #{js_runner(igniter)} vite build --config vite.config.js --mode development"
      )
      |> set_alias(
        "assets.deploy",
        [
          "cmd --cd assets #{js_runner(igniter)} vite build --config vite.config.js --mode production",
          "phx.digest"
        ]
      )
    end

    defp set_alias(igniter, task_name, command) do
      igniter
      |> Igniter.Project.TaskAliases.modify_existing_alias(
        task_name,
        fn zipper ->
          Igniter.Code.Common.replace_code(zipper, quote(do: [unquote(command)]))
        end
      )
    end

    defp configure_watchers(igniter) do
      config =
        Sourceror.parse_string!("""
        [
        #{js_runner(igniter)}: [
           "vite",
           "build",
           "--config",
           "vite.config.js",
           "--mode",
           "development",
           "--watch",
           cd: Path.expand("../assets", __DIR__)
         ]
        ]
        """)

      case Igniter.Libs.Phoenix.select_endpoint(igniter) do
        {igniter, nil} ->
          igniter

        {igniter, module} ->
          igniter
          |> Igniter.Project.Config.configure(
            "dev.exs",
            app_name(igniter),
            [module, :watchers],
            {:code, config}
          )
      end
    end

    defp configure_package_manager(igniter) do
      if System.find_executable("pnpm") && Keyword.get(igniter.args.options, :sync_pnpm, true) do
        igniter
        |> Igniter.add_notice("Using pnpm as package manager")
        |> Igniter.assign(:package_manager, :pnpm)
      else
        if System.find_executable("npm") do
          igniter
          |> Igniter.add_notice("Using npm as package manager")
          |> Igniter.assign(:package_manager, :npm)
        else
          igniter
          |> Igniter.add_issue("Cannot find suitable package manager: please install pnpm or npm")
        end
      end
    end

    defp install_assets(igniter) do
      igniter
      |> Igniter.create_or_update_file(
        "assets/package.json",
        render_template(igniter, "assets/package.json"),
        fn src ->
          Rewrite.Source.update(src, :content, fn _content ->
            render_template(igniter, "assets/package.json")
          end)
        end
      )
      |> create_new_file("assets/vite.config.ts")
      |> create_new_file("assets/tsconfig.node.json")
      |> create_new_file("assets/tsconfig.app.json")
      |> create_new_file("assets/tsconfig.json")
      |> create_or_replace_file("assets/tailwind.config.js")
      |> create_new_file("assets/js/db/auth.ts")
      |> create_new_file("assets/js/db/collections.ts")
      |> create_new_file("assets/js/db/mutations.ts")
      |> create_new_file("assets/js/db/schema.ts")
      |> create_new_file("assets/js/routes/__root.tsx")
      |> create_new_file("assets/js/routes/index.tsx")
      |> create_new_file("assets/js/routes/about.tsx")
      |> create_new_file("assets/js/api.ts")
      |> create_new_file("assets/js/app.tsx")
      |> create_or_replace_file("assets/css/app.css")
      |> Igniter.rm("assets/js/app.js")
    end

    defp create_new_file(igniter, path) do
      Igniter.create_new_file(
        igniter,
        path,
        render_template(igniter, path)
      )
    end

    defp create_or_replace_file(igniter, path, template_path \\ nil) do
      contents = render_template(igniter, template_path || path)

      igniter
      |> Igniter.create_or_update_file(
        path,
        contents,
        &Rewrite.Source.update(&1, :content, fn _content -> contents end)
      )
    end

    defp render_template(igniter, path) when is_binary(path) do
      template_contents(path, app_name: app_name(igniter) |> to_string())
    end

    @doc false
    def template_contents(path, assigns) do
      template_dir()
      |> Path.join("#{path}.eex")
      |> Path.expand(__DIR__)
      |> EEx.eval_file(assigns: assigns)
    end

    @doc false
    def template_dir do
      :phoenix_sync
      |> :code.priv_dir()
      |> Path.join("igniter/phx_sync.tanstack_db")
    end

    defp js_runner(igniter) do
      case(igniter.assigns.package_manager) do
        :pnpm -> :pnpm
        :npm -> :npx
      end
    end

    defp package_manager(igniter) do
      igniter.assigns.package_manager
    end
  end
else
  defmodule Mix.Tasks.PhxSync.TanstackDb do
    @shortdoc "#{__MODULE__.Docs.short_doc()} | Install `igniter` to use"

    @moduledoc __MODULE__.Docs.long_doc()

    use Mix.Task

    @impl Mix.Task
    def run(_argv) do
      Mix.shell().error("""
      The task 'phx_sync.tanstack_db' requires igniter. Please install igniter and try again.

      For more information, see: https://hexdocs.pm/igniter/readme.html#installation
      """)

      exit({:shutdown, 1})
    end
  end
end
