defmodule Phoenix.Sync.Client do
  alias Phoenix.Sync.PredefinedShape

  @doc """
  Create a new sync client based on the `:phoenix_sync` configuration.
  """
  def new do
    Phoenix.Sync.Application.config()
    |> new()
  end

  def new(nil) do
    new()
  end

  @doc """
  Create a sync client using the given options.

  If the integration mode is set to `:embedded` and Electric is installed
  then this will configure the client to retrieve data using the internal
  Elixir APIs.

  For the `:http` mode, then you must also configure a URL specifying 
  an Electric API server:

      config :phoenix_sync,
        electric: [
          mode: :http,
          url: "https://api.electric-sql.cloud"
        ]
  """
  def new(opts) do
    adapter = Keyword.get(opts, :adapter, Phoenix.Sync.Electric)

    apply(adapter, :client, [opts])
  end

  def new! do
    case new() do
      {:ok, client} ->
        client

      {:error, reason} ->
        raise RuntimeError, message: "Invalid client configuration: #{reason}"
    end
  end

  def new!(opts) do
    case new(opts) do
      {:ok, client} ->
        client

      {:error, reason} ->
        raise RuntimeError, message: "Invalid client configuration: #{reason}"
    end
  end

  @doc """
  Return a sync stream for the given shape.

  ## Examples

      # stream updates for the Todo schema
      Phoenix.Sync.Client.stream(MyApp.Todos.Todo)

      # stream the results of an ecto query
      Phoenix.Sync.Client.stream(from(t in MyApp.Todos.Todo, where: t.completed == true))

      # create a stream based on a shape definition
      Phoenix.Sync.Client.stream(
        table: "todos",
        where: "completed = false",
        columns: ["id", "title"]
      )
  """
  @spec stream(Phoenix.Sync.shape_definition(), Electric.Client.stream_options()) :: Enum.t()
  def stream(shape, stream_opts \\ [])

  def stream(shape, stream_opts) do
    stream(shape, stream_opts, nil)
  end

  def stream(shape, stream_opts, sync_opts) do
    client = new!(sync_opts)
    {shape, shape_stream_opts} = resolve_shape(shape)
    Electric.Client.stream(client, shape, Keyword.merge(shape_stream_opts, stream_opts))
  end

  defp resolve_shape(table) when is_binary(table) do
    {table, []}
  end

  defp resolve_shape(definition) when is_list(definition) do
    shape = PredefinedShape.new!(definition)
    PredefinedShape.to_stream_params(shape)
  end

  defp resolve_shape(schema) when is_atom(schema) do
    {schema, []}
  end

  defp resolve_shape(%Ecto.Query{} = query) do
    {query, []}
  end
end
