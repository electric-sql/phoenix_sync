defmodule Phoenix.Sync.Writer.Format.TanstackDB do
  @moduledoc """
  Implements the `Phoenix.Sync.Writer.Format` behaviour for the data format used by
  [TanStack/db](https://github.com/TanStack/db).
  """

  alias Phoenix.Sync.Writer.Transaction

  @behaviour Phoenix.Sync.Writer.Format

  @impl Phoenix.Sync.Writer.Format
  def parse_transaction([]), do: {:error, "empty transaction"}

  def parse_transaction(json) when is_binary(json) do
    with {:ok, operations} <- Jason.decode(json) do
      parse_transaction(operations)
    end
  end

  def parse_transaction(operations) when is_list(operations) do
    with {:ok, operations} <- Transaction.parse_operations(operations, &parse_operation/1) do
      Transaction.new(operations)
    end
  end

  def parse_transaction(operations) do
    {:error, "invalid operation list #{inspect(operations)}"}
  end

  def parse_operation(%{"type" => type} = m) do
    {data, changes} =
      case type do
        # for inserts we don't use the original data, just the changes
        "insert" -> {%{}, m["modified"]}
        "update" -> {m["original"], m["changes"]}
        "delete" -> {m["original"], %{}}
      end

    Transaction.operation(type, get_in(m, ["syncMetadata", "relation"]), data, changes)
  end
end
