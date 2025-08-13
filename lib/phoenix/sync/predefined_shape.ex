defmodule Phoenix.Sync.PredefinedShape do
  # A self-contained way to hold shape definition information, alongside stream
  # configuration, compatible with both the embedded and HTTP API versions.
  # Defers to the client code to validate shape options, so we can keep up with
  # changes to the api without duplicating changes here

  alias Electric.Client.ShapeDefinition

  shape_schema_gen = fn required? ->
    Keyword.take(
      [table: [type: :string, required: required?]] ++ ShapeDefinition.schema_definition(),
      ShapeDefinition.public_keys()
    )
  end

  @shape_definition_schema shape_schema_gen.(false)
  @keyword_shape_schema shape_schema_gen.(true)

  @api_schema_opts [
    storage: [type: {:or, [:map, nil]}]
  ]

  @shape_schema NimbleOptions.new!(@shape_definition_schema)
  @api_schema NimbleOptions.new!(@api_schema_opts)
  @stream_schema Electric.Client.Stream.options_schema()
  @public_schema NimbleOptions.new!(@shape_definition_schema ++ @api_schema_opts)

  @api_schema_keys Keyword.keys(@api_schema_opts)
  @stream_schema_keys Keyword.keys(@stream_schema.schema)
  @shape_definition_keys ShapeDefinition.public_keys()

  # we hold the query separate from the shape definition in order to allow
  # for transformation of a query to a shape definition at runtime rather
  # than compile time.
  defstruct [
    :shape_config,
    :api_config,
    :stream_config,
    :query
  ]

  @type t :: %__MODULE__{}
  @type option() :: unquote(NimbleOptions.option_typespec(@public_schema))
  @type options() :: [option()]

  if Code.ensure_loaded?(Ecto) do
    @type shape() :: options() | Phoenix.Sync.queryable()
  else
    @type shape() :: options()
  end

  @doc false
  def shape_schema, do: @shape_schema

  @doc false
  def schema, do: @keyword_shape_schema

  def is_queryable?(schema) when is_atom(schema) do
    Code.ensure_loaded?(schema) && function_exported?(schema, :__schema__, 1) &&
      !is_nil(schema.__schema__(:source))
  end

  def is_queryable?(q) when is_struct(q, Ecto.Query) or is_struct(q, Ecto.Changeset), do: true
  def is_queryable?(_), do: false

  @doc false
  @spec new!(shape(), options()) :: t()
  def new!(opts, config \\ [])

  def new!(shape, opts) when is_list(shape) and is_list(opts) do
    shape
    |> Keyword.merge(opts)
    |> split_and_validate_opts!(mode: :keyword)
    |> new()
  end

  def new!(table, opts) when is_binary(table) and is_list(opts) do
    new!([table: table], opts)
  end

  if Code.ensure_loaded?(Ecto) do
    def new!(ecto_shape, opts)
        when is_atom(ecto_shape) or is_struct(ecto_shape, Ecto.Query) or
               is_function(ecto_shape, 1) or
               is_struct(ecto_shape, Ecto.Changeset) do
      opts
      |> split_and_validate_opts!(mode: :ecto)
      |> Keyword.merge(query: ecto_shape)
      |> new()
    end
  end

  defp new(opts), do: struct(__MODULE__, opts)

  defp split_and_validate_opts!(opts, mode) do
    {shape_opts, other_opts} = Keyword.split(opts, @shape_definition_keys)
    {api_opts, other_opts} = Keyword.split(other_opts, @api_schema_keys)

    stream_opts =
      case Keyword.split(other_opts, @stream_schema_keys) do
        {stream_opts, []} ->
          stream_opts

        {_stream_opts, invalid_opts} ->
          raise ArgumentError,
            message: "received invalid options to a shape definition: #{inspect(invalid_opts)}"
      end

    shape_config = validate_shape_config(shape_opts, mode)
    api_config = NimbleOptions.validate!(api_opts, @api_schema)

    # remove replica value from the stream because it will override the shape
    # setting and since we've removed the `:replica` value earlier
    # it'll always be set to default
    stream_config =
      NimbleOptions.validate!(stream_opts, @stream_schema)
      |> Enum.reject(&is_nil(elem(&1, 1)))
      |> Enum.reject(&(elem(&1, 0) == :replica))

    [shape_config: shape_config, api_config: api_config, stream_config: stream_config]
  end

  # If we're defining a shape with a keyword list then we need at least the
  # `table`. Coming from some ecto value, the table is already present
  defp validate_shape_config(shape_opts, mode: :keyword) do
    NimbleOptions.validate!(shape_opts, @keyword_shape_schema)
  end

  defp validate_shape_config(shape_opts, _mode) do
    NimbleOptions.validate!(shape_opts, @shape_schema)
  end

  def client(%Electric.Client{} = client, %__MODULE__{} = predefined_shape) do
    Electric.Client.merge_params(client, to_client_params(predefined_shape))
  end

  @doc false
  def to_client_params(%__MODULE__{} = predefined_shape) do
    predefined_shape
    |> to_shape_definition()
    |> ShapeDefinition.params()
  end

  @doc false
  def to_api_params(%__MODULE__{} = predefined_shape) do
    predefined_shape
    |> to_shape_definition()
    |> ShapeDefinition.params(format: :keyword)
    |> Keyword.merge(predefined_shape.api_config)
  end

  @doc false
  def to_shape_params(%__MODULE__{} = predefined_shape) do
    predefined_shape
    |> to_shape_definition()
    |> ShapeDefinition.params(format: :keyword)
  end

  @doc false
  def to_stream_params(%__MODULE__{} = predefined_shape) do
    {to_shape_definition(predefined_shape), predefined_shape.stream_config}
  end

  defp to_shape_definition(%__MODULE__{query: nil, shape_config: shape_config}) do
    ShapeDefinition.new!(shape_config)
  end

  if Code.ensure_loaded?(Ecto) do
    # we resolve the query at runtime to avoid compile-time dependencies in
    # router modules
    defp to_shape_definition(%__MODULE__{query: queryable, shape_config: shape_config}) do
      try do
        Electric.Client.EctoAdapter.shape!(queryable, shape_config)
      rescue
        e in Protocol.UndefinedError ->
          raise ArgumentError,
            message: "Invalid query `#{inspect(queryable)}`: #{e.description}"
      end
    end
  end
end
