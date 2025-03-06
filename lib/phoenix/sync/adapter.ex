defmodule Phoenix.Sync.Adapter do
  @moduledoc false

  @callback children(atom(), keyword()) :: {:ok, [Supervisor.child_spec()]} | {:error, String.t()}
  @callback plug_opts(atom(), keyword()) :: keyword() | no_return()
  @callback client(atom(), keyword()) :: {:ok, struct()} | {:error, String.t()}
end
