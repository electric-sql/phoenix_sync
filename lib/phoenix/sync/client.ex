defmodule Phoenix.Sync.Client do
  alias Phoenix.Sync.PredefinedShape

  def new do
    Phoenix.Sync.Application.config()
    |> new()
  end

  def new(nil) do
    new()
  end

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
