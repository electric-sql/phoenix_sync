defmodule Support.Repo do
  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Ecto.Adapters.Postgres
end

defmodule Support.SandboxRepo do
  use Phoenix.Sync.Sandbox.Postgres

  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Phoenix.Sync.Sandbox.Postgres.adapter()
end

defmodule Support.ConfigTestRepo do
  use Ecto.Repo,
    otp_app: :phoenix_sync,
    adapter: Ecto.Adapters.Postgres
end

defmodule Support.RepoSetup do
  defmacro __using__(opts) do
    {:ok, repo} = Keyword.fetch(opts, :repo)

    quote do
      def with_repo_table(ctx) do
        Support.RepoSetup.with_repo_table(unquote(repo), ctx)
      end

      def with_repo_data(ctx) do
        Support.RepoSetup.with_repo_data(unquote(repo), ctx)
      end
    end
  end

  def with_repo_table(repo, ctx) do
    case ctx do
      %{table: {name, columns}} ->
        sql =
          """
          CREATE TABLE #{Support.DbSetup.inspect_relation(name)} (
          #{Enum.join(columns, ",\n")}
          )
          """

        repo.query!(sql, [])

        :ok

      _ ->
        :ok
    end

    :ok
  end

  def with_repo_data(repo, ctx) do
    case Map.get(ctx, :data, nil) do
      {schema, columns, values} ->
        Enum.each(values, fn row_values ->
          todo =
            struct(
              schema,
              Enum.zip(columns, row_values) |> Enum.map(fn {c, v} -> {String.to_atom(c), v} end)
            )

          repo.insert(todo)
        end)

        :ok

      nil ->
        :ok
    end
  end
end
