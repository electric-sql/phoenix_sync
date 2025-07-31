defmodule Phoenix.Sync.PredefinedShape do
  @moduledoc false

  # A self-contained way to hold shape definition information, alongside stream
  # configuration, compatible with both the embedded and HTTP API versions.

  alias Electric.Client.ShapeDefinition

  @keys [
    :relation,
    :where,
    :columns,
    :replica,
    :storage
  ]

  @schema NimbleOptions.new!(
            table: [type: :string],
            query: [type: {:or, [:atom, {:struct, Ecto.Query}]}, doc: false],
            namespace: [type: :string, default: "public"],
            where: [type: :string],
            columns: [type: {:list, :string}],
            replica: [type: {:in, [:default, :full]}],
            storage: [type: {:or, [:map, nil]}]
          )

  defstruct [:query | @keys]

  @type t :: %__MODULE__{}

  def schema, do: @schema
  def keys, do: @keys

  def new!(opts, config \\ [])

  def new!(shape, opts) when is_list(opts) and is_list(shape) do
    config = NimbleOptions.validate!(Keyword.merge(shape, opts), @schema)
    new(Keyword.put(config, :relation, build_relation!(config)))
  end

  def new!(schema, opts) when is_atom(schema) do
    new(Keyword.put(opts, :query, schema))
  end

  def new!(%Ecto.Query{} = query, opts) do
    new(Keyword.put(opts, :query, query))
  end

  defp new(opts) do
    struct(__MODULE__, opts)
  end

  defp build_relation!(opts) do
    build_relation(opts) ||
      raise ArgumentError,
        message: "missing relation or table in #{inspect(opts)}"
  end

  defp build_relation(opts) do
    case Keyword.get(opts, :relation) do
      {_namespace, _table} = relation ->
        relation

      nil ->
        case Keyword.get(opts, :table) do
          table when is_binary(table) ->
            namespace = Keyword.get(opts, :namespace, "public")
            {namespace, table}

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  def client(%Electric.Client{} = client, %__MODULE__{} = predefined_shape) do
    Electric.Client.merge_params(client, to_client_params(predefined_shape))
  end

  defp to_client_params(%__MODULE__{} = predefined_shape) do
    {{namespace, table}, shape} =
      predefined_shape
      |> resolve_query()
      |> to_list()
      |> Keyword.pop!(:relation)

    # Remove storage as it's not currently supported as a query param
    shape
    |> Keyword.put(:table, ShapeDefinition.url_table_name(namespace, table))
    |> Keyword.delete(:storage)
    |> columns_to_query_param()
  end

  def to_api_params(%__MODULE__{} = predefined_shape) do
    predefined_shape
    |> resolve_query()
    |> to_list()
  end

  def to_stream_params(%__MODULE__{} = predefined_shape) do
    {{namespace, table}, shape} =
      predefined_shape
      |> resolve_query()
      |> to_list()
      |> Keyword.pop!(:relation)

    {shape_opts, stream_opts} = Keyword.split(shape, ShapeDefinition.public_keys())

    {:ok, shape_definition} =
      ShapeDefinition.new(table, Keyword.merge(shape_opts, namespace: namespace))

    {shape_definition, stream_opts}
  end

  defp resolve_query(%__MODULE__{query: nil} = predefined_shape) do
    predefined_shape
  end

  # we resolve the query at runtime to avoid compile-time dependencies in
  # router modules
  defp resolve_query(%__MODULE__{} = predefined_shape) do
    from_queryable!(predefined_shape)
  end

  defp from_queryable!(%{query: queryable} = predefined_shape) do
    try do
      queryable
      |> Electric.Client.EctoAdapter.shape_from_query!()
      |> from_shape_definition(predefined_shape)
    rescue
      e in Protocol.UndefinedError ->
        raise ArgumentError,
          message: "Invalid query `#{inspect(queryable)}`: #{e.description}"
    end
  end

  defp from_shape_definition(%ShapeDefinition{} = shape_definition, predefined_shape) do
    %{
      namespace: namespace,
      table: table,
      where: where,
      columns: columns
    } = shape_definition

    %{predefined_shape | relation: {namespace || "public", table}, columns: columns}
    |> put_if(:where, where)
  end

  defp put_if(shape, _key, nil), do: shape
  defp put_if(shape, key, value), do: Map.put(shape, key, value)

  defp to_list(%__MODULE__{} = shape) do
    Enum.flat_map(@keys, fn key ->
      value = Map.fetch!(shape, key)

      if !is_nil(value),
        do: [{key, value}],
        else: []
    end)
  end

  defp columns_to_query_param(shape) do
    case Keyword.get(shape, :columns) do
      columns when is_list(columns) -> Keyword.put(shape, :columns, Enum.join(columns, ","))
      _ -> shape
    end
  end
end
