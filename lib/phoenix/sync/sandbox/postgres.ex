if Phoenix.Sync.sandbox_enabled?() do
  defmodule Phoenix.Sync.Sandbox.Postgres do
    defmacro __using__(_opts) do
      quote do
        require Phoenix.Sync.Sandbox.Postgres
      end
    end

    @doc """
    Replace hard-coded references to `Ecto.Adapters.Postgres` with this macro to
    enable the `Phoenix.Sync.Sandbox` in test mode.

    In development or production environments the repo will use the standard
    Postgres adapter directly, only if `Mix.env() == :test` (see below) will it
    use the sandbox adapter shim.

    Example:

        defmodule MyApp.Repo do
          use Phoenix.Sync.Sandbox.Postgres

          use Ecto.Repo,
            otp_app: :my_app,
            adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
        end

    ### Custom environments

    If you want to enable the sandbox adapter in different environments, you can
    use your own evaluation logic:

        defmodule MyApp.Repo do
          use Phoenix.Sync.Sandbox.Postgres

          use Ecto.Repo,
            otp_app: :my_app,
            adapter: Phoenix.Sync.Sandbox.Postgres.adapter(Mix.env() in [:test, :special])
        end

    > #### Warning {: .warning}
    >
    > The expression passed to `adapter/1` will be evaluated at **compile time**,
    > not run-time so only use expressions that can be evaluated at then (this
    > includes the various `Mix` functions).
    """
    defmacro adapter(expr \\ Mix.env() == :test) do
      {enable_sandbox?, _binding} = Code.eval_quoted(expr, binding())

      if enable_sandbox?,
        do: Phoenix.Sync.Sandbox.Postgres.Adapter,
        else: Ecto.Adapters.Postgres
    end
  end
end
