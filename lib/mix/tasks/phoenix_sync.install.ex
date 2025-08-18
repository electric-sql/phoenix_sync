defmodule Mix.Tasks.PhoenixSync.Install.Docs do
  @moduledoc false

  @spec short_doc() :: String.t()
  def short_doc do
    "Install Phoenix.Sync into an existing Phoenix or Plug application"
  end

  @spec example() :: String.t()
  def example do
    "mix phoenix_sync.install --mode embedded --repo MyApp.Repo"
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

    * `--mode`- How to connect to Electric, either `embedded` or `http`. In `embedded` mode `:electric` will be added as a dependency and started automatically and you may have to include the `--repo` option (see below). For `http` mode you'll need to specify the `--url` to the Electric server.
    * `--repo`- Which `Ecto.Repo` module to connect to in `embedded` mode. The installer should be able to find the right repo automatically, but if it can't, you can specify it here.
    * `--url` - The URL of the Electric server, only needed for `--mode http`.
    """
  end
end

if Code.ensure_loaded?(Igniter) do
  defmodule Mix.Tasks.PhoenixSync.Install do
    @shortdoc "#{__MODULE__.Docs.short_doc()}"

    @moduledoc __MODULE__.Docs.long_doc()

    use Igniter.Mix.Task

    @impl Igniter.Mix.Task
    def info(argv, composing_task) do
      {opts, _positional, _other} = OptionParser.parse(argv, strict: [mode: :string])

      adds_deps =
        case Keyword.fetch(opts, :mode) do
          {:ok, "embedded"} ->
            [{:electric, required_electric_version()}]

          {:ok, "http"} ->
            []

          {:ok, _other} ->
            # errors will come later
            []
        end

      %Igniter.Mix.Task.Info{
        # Groups allow for overlapping arguments for tasks by the same author
        # See the generators guide for more.
        group: :phoenix_sync,
        # *other* dependencies to add
        # i.e `{:foo, "~> 2.0"}`
        adds_deps: adds_deps,
        # *other* dependencies to add and call their associated installers, if they exist
        # i.e `{:foo, "~> 2.0"}`
        installs: [],
        # An example invocation
        example: __MODULE__.Docs.example(),
        # A list of environments that this should be installed in.
        only: nil,
        # a list of positional arguments, i.e `[:file]`
        positional: [],
        # Other tasks your task composes using `Igniter.compose_task`, passing in the CLI argv
        # This ensures your option schema includes options from nested tasks
        composes: [],
        # `OptionParser` schema
        schema: [
          mode: :string,
          repo: :string,
          url: :string,
          sandbox: :boolean
        ],
        # Default values for the options in the `schema`
        defaults: [],
        # CLI aliases
        aliases: [],
        # A list of options in the schema that are required
        required: [:mode]
      }
    end

    @valid_modes ~w(embedded http)

    @impl Igniter.Mix.Task
    def igniter(igniter) do
      {:ok, mode} = Keyword.fetch(igniter.args.options, :mode)

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
      case Keyword.fetch(igniter.args.options, :url) do
        {:ok, url} ->
          igniter
          |> base_configuration(:http)
          |> Igniter.Project.Config.configure_new(
            "config.exs",
            :phoenix_sync,
            [:url],
            url
          )

        :error ->
          Igniter.add_issue(igniter, "`--url` is required for :http mode")
      end
    end

    defp add_dependencies(igniter, "embedded") do
      # we should maybe confirm all this stuff before doing it

      igniter
      |> base_configuration(:embedded)
      |> find_repo()
      |> configure_repo()

      # update the repo source with the sandbox adapter
      # check if this is phoenix or plain plug
      # add the plug_opts
    end

    defp configure_repo(%{issues: [_ | _] = _issues} = igniter) do
      igniter
    end

    defp configure_repo(igniter) do
      enable_sandbox? = Keyword.get(igniter.args.options, :sandbox, true)

      igniter =
        igniter
        |> Igniter.Project.Config.configure_new(
          "config.exs",
          :phoenix_sync,
          [:repo],
          igniter.assigns.repo
        )

      if enable_sandbox? do
        igniter
        |> Igniter.Project.Module.find_and_update_module!(
          igniter.assigns.repo |> dbg,
          fn zipper ->
            with {:ok, zipper} <-
                   Igniter.Code.Module.move_to_use(zipper, Ecto.Repo) |> dbg,
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
      app_name = Igniter.Project.Application.app_name(igniter) |> dbg

      case Application.fetch_env(app_name, :ecto_repos) do
        {:ok, [repo | _]} ->
          Igniter.assign(igniter, :repo, repo)

        :error ->
          Igniter.add_issue(
            igniter,
            "No Ecto.Repo found in application environment, please specify one using `--repo` option"
          )
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
