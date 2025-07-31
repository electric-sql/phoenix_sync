defmodule Phoenix.Sync.Client do
  @moduledoc """
  Low level Elixir client. Converts an `Ecto.Query` into an Elixir `Stream`:

  ```elixir
  stream = Phoenix.Sync.Client.stream(Todos.Todo)

  stream =
    Ecto.Query.from(t in Todos.Todo, where: t.completed == false)
    |> Phoenix.Sync.Client.stream()
  ```
  """

  alias Phoenix.Sync.PredefinedShape

  @doc """
  Create a new sync client based on the `:phoenix_sync` configuration.
  """
  def new do
    Phoenix.Sync.Application.config() |> new()
  end

  def new(nil) do
    new()
  end

  @doc """
  Create a sync client using the given options.

  If the integration mode is set to `:embedded` and Electric is installed
  then this will configure the client to retrieve data using the internal
  Elixir APIs.

  For the `:http` mode, then you must also configure a URL specifying an
  Electric API server:

      config :phoenix_sync,
        mode: :http,
        url: "https://api.electric-sql.cloud"

  This client can then generate streams for use in your Elixir applications:

      {:ok, client} = Phoenix.Sync.Client.new()
      stream = Electric.Client.stream(client, Todos.Todo)
      for msg <- stream, do: IO.inspect(msg)

  Alternatively use `stream/1` which wraps this functionality.
  """
  def new(opts) do
    {adapter, env} = Phoenix.Sync.Application.adapter_env(opts)

    apply(adapter, :client, [env, opts])
  end

  @doc """
  Create a new sync client based on the application configuration or raise if
  the config is invalid.

      client = Phoenix.Sync.Client.new!()

  See `new/0`.
  """
  def new! do
    case new() do
      {:ok, client} ->
        client

      {:error, reason} ->
        raise RuntimeError, message: "Invalid client configuration: #{reason}"
    end
  end

  @doc """
  Create a new sync client based on the given opts or raise if
  the config is invalid.

      client = Phoenix.Sync.Client.new!(mode: :embedded)

  See `new/1`.
  """
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
      stream = Phoenix.Sync.Client.stream(MyApp.Todos.Todo)

      # stream the results of an ecto query
      stream = Phoenix.Sync.Client.stream(from(t in MyApp.Todos.Todo, where: t.completed == true))

      # create a stream based on a shape definition
      stream = Phoenix.Sync.Client.stream(
        table: "todos",
        where: "completed = false",
        columns: ["id", "title"]
      )

      # consumer a pre-defined shape from a remote server, e.g.
      # one defined using `Phoenix.Sync.Controller.sync_render/3` or
      # `Phoenix.Sync.Router.sync/2`.
      stream = Phoenix.Sync.Client.stream(
        "https://myapp.com/sync/todos?user_id=1234"
      )

      # once you have a stream, consume it as usual
      Enum.each(stream, &IO.inspect/1)

  ## Ecto vs keyword shapes

  Streams defined using an Ecto query or schema will return data wrapped in
  the appropriate schema struct, with values cast to the appropriate
  Elixir/Ecto types, rather than raw column data in the form `%{"column_name"
  => "column_value"}`.
  """
  # stream(Todo, replica: full, client: client)
  # stream("todos", where: "...", replica: full, client: client)
  # stream(table: "todos", where: "...", replica: full, client: client)
  @spec stream(Phoenix.Sync.shape_definition(), Electric.Client.stream_options()) :: Enum.t()
  def stream(shape, stream_opts \\ [])

  def stream(shape, []) when is_list(shape) do
    {client, shape} = Keyword.pop_lazy(shape, :client, &new!/0)

    {shape, shape_stream_opts} = resolve_shape(shape)

    Electric.Client.stream(client, shape, shape_stream_opts)
  end

  def stream(table, stream_opts) when is_binary(table) and is_list(stream_opts) do
    stream(Keyword.put(stream_opts, :table, table), [])
  end

  def stream(shape, stream_opts) when not is_list(shape) and is_list(stream_opts) do
    {client, stream_opts} = Keyword.pop_lazy(stream_opts, :client, &new!/0)

    {shape, shape_stream_opts} = resolve_shape(shape)
    stream_opts = Keyword.merge(shape_stream_opts, stream_opts)
    Electric.Client.stream(client, shape, stream_opts)
  end

  defp resolve_shape(shape, stream_opts) do
    shape
    |> PredefinedShape.new!(stream_opts)
    |> Phoenix.Sync.PredefinedShape.to_stream_params()
  end
end
