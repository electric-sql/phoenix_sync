defmodule Phoenix.Sync.Write.Mutation do
  @moduledoc """
  Represents a mutation received from a client.

  To handle custom formats, translate the received JSON into a `Mutation`
  struct using `new/4`.
  """
  defstruct [:type, :relation, :data, :changes]

  @type t() :: %__MODULE__{
          type: :insert | :update | :delete,
          relation: binary() | [binary(), ...],
          data: map(),
          changes: map()
        }

  @type new_result() :: {:ok, t()} | {:error, String.t()}
  @doc """
  Takes data from a mutation and validates it before returning a struct.

  ## Parameters

  - `action` one of `"insert"`, `"update"` or `"delete"`.
  - `table` the client table name for the write. Can either be a plain string
    name or a list with `["schema", "table"]`.
  - `data` the original values (see ["Updates vs Inserts vs Deletes"](#new/4-updates-vs-inserts-vs-deletes))
  - `changes` any updates to apply (see ["Updates vs Inserts vs Deletes"](#new/4-updates-vs-inserts-vs-deletes))

  ## Updates vs Inserts vs Deletes

  The `Mutation` struct has two value fields, `data` and `changes`.

  `data` represents what's already in the database, and `changes` what's
  going to be written over the top of this.

  For `insert` operations, `data` is ignored so the new values for the
  inserted row should be in `changes`.

  For `deletes`, `changes` is ignored and `data` should contain the row
  specification to delete. This needn't be the full row, but must contain
  values for all the **primary keys** for the table.

  For `updates`, `data` should contain the original row value and `changes`
  the changed fields.

  These fields map to the arguments `Ecto.Changeset.change/2` and
  `Ecto.Changeset.cast/4` functions, `data` is used to populate the first
  argument of these functions and `changes` the second.
  """
  @spec new(binary(), binary() | [binary(), ...], map() | nil, map() | nil) :: new_result()
  def new(action, table, data, changes) do
    with {:ok, type} <- validate_action(action),
         {:ok, relation} <- validate_table(table),
         {:ok, data} <- validate_data(data, type),
         {:ok, changes} <- validate_changes(changes, type) do
      {:ok, %__MODULE__{type: type, relation: relation, data: data, changes: changes}}
    end
  end

  @doc """
  Parses the mutation data returned from
  [TanStack/optimistic](https://github.com/TanStack/optimistic).
  """
  @spec tanstack(%{binary() => binary() | map()}) :: new_result()
  def tanstack(%{"type" => type} = m) do
    {data, changes} =
      case type do
        # for inserts we don't use the original data, just the changes
        "insert" -> {%{}, m["modified"]}
        "update" -> {m["original"], m["changes"]}
        "delete" -> {m["original"], %{}}
      end

    new(type, get_in(m, ["syncMetadata", "relation"]), data, changes)
  end

  defp validate_action("insert"), do: {:ok, :insert}
  defp validate_action("update"), do: {:ok, :update}
  defp validate_action("delete"), do: {:ok, :delete}

  defp validate_action(type),
    do:
      {:error,
       "Invalid action #{inspect(type)} expected one of #{inspect(~w(insert update delete))}"}

  defp validate_table(name) when is_binary(name), do: {:ok, name}

  defp validate_table([prefix, name] = relation) when is_binary(name) and is_binary(prefix),
    do: {:ok, relation}

  defp validate_table(table), do: {:error, "Invalid table: #{inspect(table)}"}

  defp validate_data(_, :insert), do: {:ok, %{}}
  defp validate_data(%{} = data, _), do: {:ok, data}

  defp validate_data(data, _type),
    do: {:error, "Invalid data expected map got #{inspect(data)}"}

  defp validate_changes(_, :delete), do: {:ok, %{}}
  defp validate_changes(%{} = changes, _insert_or_update), do: {:ok, changes}

  defp validate_changes(changes, _insert_or_update),
    do: {:error, "Invalid changes for update. Expected map got #{inspect(changes)}"}
end
