defmodule Phoenix.Sync.Sandbox.SchemaReconciler do
  @moduledoc false

  use GenServer

  def start_link(_args), do: :ignore
  def init(_arg), do: :ignore
  def reconcile_now(_name_or_pid), do: :ok
end
