defmodule Phoenix.Sync.Write.Mutation do
  @moduledoc """
  Represents a mutation received from a client.

  To handle custom formats, translate incoming changes into a `Mutation`
  struct using `new/4`.

  ## Custom data formats

  The exact format of the change messages coming from the client is
  unimportant, however `Phoenix.Sync.Write` requires certain essential
  information that needs to be included.

  - `operation` - one of `insert`, `update` or `delete`
  - `relation` - the table name that the operation applies to.
  - `data` - the original data before the mutation was applied. **Required** for
    `update` or `delete` operations
  - `changes` - the changes to apply. **Required** for `insert` and `update` operations

  However you choose to encode this information on the client, you simply need
  to override the `parser` option of your write configuration using
  `Phoenix.Sync.Write.new/1` to a 1-arity function that takes the data from the
  client and returns a `%Phoenix.Sync.Write.Mutation{}` struct by calling `new/4`.

  ### Example:

  We use Protocol buffers to encode the incoming information using :protox

      defmodule MyApp.Protocol do
        use Protox,
          namespace: __MODULE__,
          schema: ~S[
            syntax: "proto2";
            message Operation {
              enum Action {
                INSERT = 0;
                UPDATE = 1;
                DELETE = 2;
              }
              required Action action = 1;
              string table = 2;
              map<string, string> original = 3;
              map<string, string> modified = 4;
            }
            message Transaction {
              repeated Operation operations = 1;
            }
          ]
      end

  Then when creating our writer we pass in a custom `parser/1` function:


      # first decode the transaction
      %MyApp.Protocol.Transaction{operations: operations} = MyApp.Protocol.Transaction.decode!(data)

      writer =
        Phoenix.Sync.Write.new(parser: fn %MyApp.Protocol.Operation{} = op ->
          %{action: action, table: table, original: original, modified: modified} = op

          # protobuf enums are uppercase atoms
          action
          |> to_string()
          |> String.downcase()
          |> Phoenix.Sync.Write.Mutation.new(table, original, modified)
        end)
  """

  defstruct [:operation, :relation, :data, :changes]

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

  - `operation` one of `"insert"`, `"update"` or `"delete"`.
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

  @doc """
  Parses the mutation data returned from
  [TanStack/optimistic](https://github.com/TanStack/optimistic).
  """
  @spec tanstack(%{binary() => any()}) :: new_result()
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

  defp validate_operation("insert"), do: {:ok, :insert}
  defp validate_operation("update"), do: {:ok, :update}
  defp validate_operation("delete"), do: {:ok, :delete}
  defp validate_operation(o) when o in [:insert, :update, :delete], do: {:ok, o}

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
