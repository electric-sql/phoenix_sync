{:ok, _} = Support.SandboxRepo.start_link()
{:ok, _} = Phoenix.Sync.LiveViewTest.Endpoint.start_link()

# Exclude tests that require external dependencies:
# - :sandbox - sandbox mode is deprecated in Electric 1.2.x
# - :igniter - requires phx_new archive to be installed (available in CI)
# Run with `mix test --include sandbox` or `mix test --include igniter` to include them
ExUnit.start(capture_log: true, exclude: [:sandbox, :igniter])
