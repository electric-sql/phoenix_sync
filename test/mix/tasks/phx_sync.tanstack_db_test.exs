defmodule Mix.Tasks.PhxSync.TanstackDbTest do
  use ExUnit.Case, async: true
  import Igniter.Test

  test "it warns when run" do
    # generate a test project
    test_project()
    # run our task
    |> Igniter.compose_task("phx_sync.tanstack_db", [])
    # see tools in `Igniter.Test` for available assertions & helpers
    |> assert_has_warning("mix phx_sync.tanstack_db is not yet implemented")
  end
end
