defmodule Mix.Tasks.PhoenixSync.InstallTest do
  use ExUnit.Case, async: true
  import Igniter.Test

  test "it warns when run" do
    # generate a test project
    test_project()
    # run our task
    |> Igniter.compose_task("phoenix_sync.install", [])
    # see tools in `Igniter.Test` for available assertions & helpers
    |> assert_has_warning("mix phoenix_sync.install is not yet implemented")
  end
end
