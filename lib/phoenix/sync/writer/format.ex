defmodule Phoenix.Sync.Writer.Format do
  @moduledoc """
  Defines a behaviour that applications can implement in order to handle custom
  data formats within a `Phoenix.Sync.Writer`

  The exact format of the change messages coming from the client is
  unimportant, however `Phoenix.Sync.Writer` requires certain essential
  information that needs to be included.

  - `operation` - one of `insert`, `update` or `delete`
  - `relation` - the table name that the operation applies to.
  - `data` - the original data before the mutation was applied. **Required** for
    `update` or `delete` operations
  - `changes` - the changes to apply. **Required** for `insert` and `update` operations

  However you choose to encode this information on the client, you simply need
  to set the `format` of your write configuration using to a module that
  implements this behaviour.

  #### Implementation guide

  Once you have parsed the data coming from the client, over HTTP or even raw
  TCP, use `Phoenix.Sync.Writer.Operation.new/4` (or
  `Phoenix.Sync.Writer.Transaction.operation/4`) to validate the values and
  create a `%Phoenix.Sync.Writer.Operation{}` struct.

  `Phoenix.Sync.Writer.Transaction.parse_operations/2` is a helper function for
  error handling when mapping a list of encoded operation data using
  `Phoenix.Sync.Writer.Transaction.operation/4`.

  ### Example:

  We use Protocol buffers to encode the incoming information using `:protox`
  and implement this module's `c:parse_transaction/1` callback using the `Protox` decode functions:

      defmodule MyApp.Protocol do
        use Protox,
          namespace: __MODULE__,
          schema: ~S[
            syntax: "proto2";
            message Operation {
              enum Op {
                INSERT = 0;
                UPDATE = 1;
                DELETE = 2;
              }
              required Op op = 1;
              string table = 2;
              map<string, string> original = 3;
              map<string, string> modified = 4;
            }
            message Transaction {
              repeated Operation operations = 1;
            }
          ]

        alias Phoenix.Sync.Writer

        @behaviour #{inspect(__MODULE__)}

        @impl #{inspect(__MODULE__)}
        def parse_transaction(proto) when is_binary(proto) do
          with {:ok, %{operations: ops}} <- MyApp.Protocol.Transaction.decode(proto),
               {:ok, operations} <- Writer.Transaction.parse_operations(ops, &convert_operation/1) do
            {:ok, %Writer.Transaction{operations: operations}
          end
        end

        defp convert_operation(%MyApp.Protocol.Operation{} = operation) do
          Writer.Transaction.operation(
            operation.op,
            operation.table,
            operation.original,
            operation.modified
          )
        end
      end

  We use our protocol module as the `format` when we `apply/4` our transaction
  data and we can just pass our serialized protobuf message as the mutation data:

      {:ok, txid, _changes} =
        Phoenix.Sync.Writer.new()
        |> Phoenix.Sync.Writer.apply(protobuf_data, MyApp.Repo, format: MyApp.Protocol)
  """
  alias Phoenix.Sync.Writer.Transaction

  @type t() :: module()
  @typedoc "Raw data from a client that will be parsed to a `#{inspect(__MODULE__.Transaction)}` by the Writer's format parser"
  @type transaction_data() :: term()
  @type parse_transaction_result() :: {:ok, Transaction.t()} | {:error, term()}
  @type parser_fun() :: (transaction_data() -> parse_transaction_result()) | mfa()

  @doc """
  Translate some data format into a `Phoenix.Sync.Writer.Transaction` with a
  list of operations to apply.
  """
  @callback parse_transaction(term()) :: parse_transaction_result()
end
