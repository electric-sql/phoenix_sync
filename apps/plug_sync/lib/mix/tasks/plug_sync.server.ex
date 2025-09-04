defmodule Mix.Tasks.PlugSync.Server do
  use Mix.Task

  @shortdoc "Starts a PlugSync server"

  def run(_args) do
    Application.put_env(:plug_sync, :server, true, persistent: true)
    Mix.Tasks.Run.run(run_args())
  end

  defp run_args do
    if iex_running?(), do: [], else: ["--no-halt"]
  end

  defp iex_running? do
    Code.ensure_loaded?(IEx) and IEx.started?()
  end
end
