defmodule Phoenix.Sync.Writer.Transaction do
  defstruct txid: nil, operations: []

  alias Phoenix.Sync.Writer.Operation

  @type id() :: integer()
  @type t() :: %__MODULE__{
          txid: nil | id(),
          operations: [Operation.t(), ...]
        }

  @doc """
  Return a new, empty, `Transaction` struct.
  """
  @spec empty() :: t()
  def empty, do: %__MODULE__{}

  @spec new([Operation.t()]) :: {:ok, t()} | {:error, term()}
  def new([]) do
    {:error, "empty transaction"}
  end

  def new(operations) when is_list(operations) do
    if Enum.all?(operations, &match?(%Operation{}, &1)) do
      operations =
        operations
        |> Enum.with_index()
        |> Enum.map(fn {operation, idx} -> %{operation | index: idx} end)

      {:ok, %__MODULE__{operations: operations}}
    else
      {:error, "Invalid operations list"}
    end
  end

  defdelegate operation(operation, table, data, changes), to: Operation, as: :new
  defdelegate operation!(operation, table, data, changes), to: Operation, as: :new!

  @doc """
  Helper function to parse a list of encoded Operations.
  """
  @spec parse_operations([term()], (term() -> {:ok, Operation.t()} | {:error, term()})) ::
          {:ok, [Operation.t()]} | {:error, term()}
  def parse_operations(raw_operations, parse_function) when is_function(parse_function, 1) do
    with operations when is_list(operations) <-
           do_parse_operations(raw_operations, parse_function) do
      {:ok, Enum.reverse(operations)}
    end
  end

  defp do_parse_operations(raw_operations, parse_function) do
    raw_operations
    |> Enum.reduce_while([], fn raw_op, operations ->
      case parse_function.(raw_op) do
        {:ok, operation} ->
          {:cont, [operation | operations]}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end
end
