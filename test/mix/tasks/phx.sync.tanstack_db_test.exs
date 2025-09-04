defmodule Mix.Tasks.Phx.Sync.TanstackDbTest do
  use ExUnit.Case, async: true

  import Igniter.Test

  import Mix.Tasks.Phx.Sync.TanstackDb, only: [template_dir: 0, template_contents: 2]

  defp assert_renders_template(igniter, {template_path, render_path}) do
    igniter
    |> assert_content_equals(render_path, template_contents(template_path, app_name: "test"))
  end

  test "package.json" do
    json = template_contents("assets/package.json", app_name: "test")
    assert json =~ ~r("type": "module")
    assert json =~ ~r("name": "test")
    assert json =~ ~r("version": "0.0.0")
  end

  test "installs assets from templates" do
    templates =
      template_dir()
      |> Path.join("**/*.*")
      |> Path.wildcard()
      |> Enum.map(&Path.relative_to(&1, template_dir()))
      |> Enum.map(fn path ->
        dir =
          Path.dirname(path)
          |> String.replace(~r/^\.$/, "")

        render_dir =
          dir |> String.replace(~r|lib/web/components|, "lib/test_web/components")

        file = Path.basename(path)

        file = String.replace(file, ~r/\.eex$/, "")

        {Path.join(dir, file), Path.join(render_dir, file)}
      end)

    igniter =
      phx_test_project()
      |> Igniter.compose_task("phx.sync.tanstack_db", [])

    for template <- templates do
      assert_renders_template(igniter, template)
    end
  end

  test "patches tasks" do
    igniter =
      phx_test_project()
      |> Igniter.compose_task("phx.sync.tanstack_db", ["--sync-pnpm"])

    assert_has_patch(igniter, "mix.exs", """
    - |      {:esbuild, "~> 0.8", runtime: Mix.env() == :dev},
    - |      {:tailwind, "~> 0.2", runtime: Mix.env() == :dev},
    """)

    assert_has_patch(igniter, "mix.exs", """
    - |      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
    - |      "assets.build": ["tailwind test", "esbuild test"],
    + |      "assets.setup": ["cmd --cd assets pnpm install --ignore-workspace"],
    + |      "assets.build": [
    + |        "cmd --cd assets pnpm vite build --config vite.config.js --mode development"
    + |      ],
    """)
  end

  test "configures watchers in dev" do
    igniter =
      phx_test_project()
      |> Igniter.compose_task("phx.sync.tanstack_db", ["--sync-pnpm"])

    assert_has_patch(igniter, "config/dev.exs", """
    - |    esbuild: {Esbuild, :install_and_run, [:test, ~w(--sourcemap=inline --watch)]},
    - |    tailwind: {Tailwind, :install_and_run, [:test, ~w(--watch)]}
    + |    pnpm: [
    + |      "vite",
    + |      "build",
    + |      "--config",
    + |      "vite.config.js",
    + |      "--mode",
    + |      "development",
    + |      "--watch",
    + |      cd: Path.expand("../assets", __DIR__)
    + |    ]
    """)
  end

  test "uses npm if told" do
    igniter =
      phx_test_project()
      |> Igniter.compose_task("phx.sync.tanstack_db", ["--no-sync-pnpm"])

    assert_has_patch(igniter, "config/dev.exs", """
    - |    esbuild: {Esbuild, :install_and_run, [:test, ~w(--sourcemap=inline --watch)]},
    - |    tailwind: {Tailwind, :install_and_run, [:test, ~w(--watch)]}
    + |    npx: [
    + |      "vite",
    + |      "build",
    + |      "--config",
    + |      "vite.config.js",
    + |      "--mode",
    + |      "development",
    + |      "--watch",
    + |      cd: Path.expand("../assets", __DIR__)
    + |    ]
    """)
  end

  test "removes app.js" do
    igniter =
      phx_test_project()
      |> Igniter.compose_task("phx.sync.tanstack_db", ["--sync-pnpm"])

    assert_rms(igniter, ["assets/js/app.js"])
  end
end
