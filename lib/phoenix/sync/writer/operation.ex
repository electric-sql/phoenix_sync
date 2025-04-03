defmodule Phoenix.Sync.Writer.Operation do
  @moduledoc """
  Represents a mutation operation received from a client.

  To handle custom formats, translate incoming changes into a [`Operation`](`#{inspect(__MODULE__)}`)
  struct using `new/4`.

  """

  defstruct [:index, :operation, :relation, :data, :changes]

  order = %{insert: 0, update: 1, delete: 2}

  sort_mapper = fn {op, _} -> order[op] end

  equivalent_ops = fn op ->
    upper = op |> to_string() |> String.upcase()
    [op, to_string(op), upper, String.to_atom(upper)]
  end

  @accepted_operations Map.new([:insert, :update, :delete], &{&1, equivalent_ops.(&1)})

  allowed_values =
    @accepted_operations
    |> Enum.sort_by(sort_mapper)
    |> Enum.flat_map(&Enum.sort(elem(&1, 1), :desc))
    |> Enum.map(&"`#{inspect(&1)}`")
    |> Enum.join(",")

  @type t() :: %__MODULE__{
          operation: :insert | :update | :delete,
          relation: binary() | [binary(), ...],
          data: map(),
          changes: map()
        }

  @type new_result() :: {:ok, t()} | {:error, String.t()}
  @doc """
  Takes data from a mutation and validates it before returning a struct.

  ## Parameters

  - `operation` one of #{allowed_values}
  - `table` the client table name for the write. Can either be a plain string
    name `"table"` or a list with `["schema", "table"]`.
  - `data` the original values (see [Updates vs Inserts vs Deletes](#new/4-updates-vs-inserts-vs-deletes))
  - `changes` any updates to apply (see [Updates vs Inserts vs Deletes](#new/4-updates-vs-inserts-vs-deletes))

  ## Updates vs Inserts vs Deletes

  The `#{inspect(__MODULE__)}` struct has two value fields, `data` and `changes`.

  `data` represents what's already in the database, and `changes` what's
  going to be written over the top of this.

  For `insert` operations, `data` is ignored so the new values for the
  inserted row should be in `changes`.

  For `deletes`, `changes` is ignored and `data` should contain the row
  specification to delete. This needn't be the full row, but must contain
  values for all the **primary keys** for the table.

  For `updates`, `data` should contain the original row values and `changes`
  the changed fields.

  These fields map to the arguments `Ecto.Changeset.change/2` and
  `Ecto.Changeset.cast/4` functions, `data` is used to populate the first
  argument of these functions and `changes` the second.
  """
  @spec new(binary() | atom(), binary() | [binary(), ...], map() | nil, map() | nil) ::
          new_result()
  def new(operation, table, data, changes) do
    with {:ok, operation} <- validate_operation(operation),
         {:ok, relation} <- validate_table(table),
         {:ok, data} <- validate_data(data, operation),
         {:ok, changes} <- validate_changes(changes, operation) do
      {:ok, %__MODULE__{operation: operation, relation: relation, data: data, changes: changes}}
    end
  end

  @spec new!(binary() | atom(), binary() | [binary(), ...], map() | nil, map() | nil) :: t()
  def new!(operation, table, data, changes) do
    case new(operation, table, data, changes) do
      {:ok, operation} -> operation
      {:error, reason} -> raise ArgumentError, message: reason
    end
  end

  for {operation, equivalents} <- @accepted_operations, valid <- equivalents do
    defp validate_operation(unquote(valid)), do: {:ok, unquote(operation)}
  end

  defp validate_operation(op),
    do:
      {:error,
       "Invalid operation #{inspect(op)} expected one of #{inspect(~w(insert update delete))}"}

  defp validate_table(name) when is_binary(name), do: {:ok, name}

  defp validate_table([prefix, name] = relation) when is_binary(name) and is_binary(prefix),
    do: {:ok, relation}

  defp validate_table(table), do: {:error, "Invalid table: #{inspect(table)}"}

  defp validate_data(_, :insert), do: {:ok, %{}}
  defp validate_data(%{} = data, _), do: {:ok, data}

  defp validate_data(data, _op),
    do: {:error, "Invalid data expected map got #{inspect(data)}"}

  defp validate_changes(_, :delete), do: {:ok, %{}}
  defp validate_changes(%{} = changes, _insert_or_update), do: {:ok, changes}

  defp validate_changes(changes, _insert_or_update),
    do: {:error, "Invalid changes for update. Expected map got #{inspect(changes)}"}
end
