defmodule Phoenix.Sync.Plug.Utils do
  @moduledoc false

  @doc false
  def env!(caller) do
    cond do
      Macro.Env.required?(caller, Plug.Router) ->
        :plug

      Macro.Env.required?(caller, Phoenix.Router) ->
        :phoenix

      true ->
        raise ArgumentError,
          message: "Unrecognised compilation environment, neither Plug.Router nor Phoenix.Router"
    end
  end

  @doc false
  def opts_in_assign!(opts, using_module, plug_module) do
    opts_in_assign = opts[:opts_in_assign]

    builder_opts = Module.get_attribute(using_module, :plug_builder_opts)
    assign = builder_opts[:copy_opts_to_assign] || opts_in_assign

    if !assign do
      raise ArgumentError,
        message:
          "To use #{plug_module} you must configure Plug.Router with :copy_opts_to_assign <https://hexdocs.pm/plug/Plug.Builder.html#module-options>"
    end

    assign
  end
end
