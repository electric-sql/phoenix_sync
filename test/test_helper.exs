{:ok, _} = Support.SandboxRepo.start_link()
{:ok, _} = Phoenix.Sync.Test.Sandbox.start_link()
{:ok, _} = Phoenix.Sync.LiveViewTest.Endpoint.start_link()

ExUnit.start(capture_log: true)
