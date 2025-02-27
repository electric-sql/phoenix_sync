defmodule Phoenix.Sync.Client do
  alias Phoenix.Sync.PredefinedShape

  @valid_modes Phoenix.Sync.Application.valid_modes() -- [:disabled]

  def new do
    Phoenix.Sync.Application.config()
    |> new()
  end

  def new(nil) do
    new()
  end

  def new(opts) do
    case Keyword.fetch(opts, :mode) do
      {:ok, mode} when mode in @valid_modes ->
        configure_client(Keyword.get(opts, :electric, []), mode)

      {:ok, invalid_mode} ->
        {:error, "Cannot configure client for mode #{inspect(invalid_mode)}"}

      :error ->
        if Phoenix.Sync.Application.electric_available?() do
          configure_client(Keyword.get(opts, :electric, []), :embedded)
        else
          {:error, "No mode configured"}
        end
    end
  end

  if Phoenix.Sync.Application.electric_available?() do
    defp configure_client(opts, :embedded) do
      Electric.Client.embedded(opts)
    end
  else
    defp configure_client(_opts, :embedded) do
      {:error, "electric not installed, unable to created embedded client"}
    end
  end

  defp configure_client(opts, :http) do
    case Keyword.fetch(opts, :url) do
      {:ok, url} ->
        Electric.Client.new(base_url: url)

      :error ->
        {:error, "`phoenix_sync[:electric][:url]` not set for phoenix_sync in HTTP mode"}
    end
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
