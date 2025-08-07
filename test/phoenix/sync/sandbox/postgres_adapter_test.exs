defmodule Phoenix.Sync.Sandbox.PostgresAdapterTest do
  use ExUnit.Case, async: true

  describe "adapter/1" do
    defmodule Adapter do
      import Phoenix.Sync.Sandbox.Postgres, only: [adapter: 0, adapter: 1]

      def default, do: adapter()
      def prod, do: adapter(Mix.env() == :prod)
      def always, do: adapter(true)
    end

    test "defaults to testing for :test env" do
      assert Adapter.default() == Phoenix.Sync.Sandbox.Postgres.Adapter
    end

    test "allows for custom test conditions" do
      assert Adapter.prod() == Ecto.Adapters.Postgres
    end

    test "accepts hard-coded bools" do
      assert Adapter.always() == Phoenix.Sync.Sandbox.Postgres.Adapter
    end
  end
end
