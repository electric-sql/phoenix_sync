defmodule Phoenix.SyncTest do
  use ExUnit.Case, async: true

  import Plug.Test

  doctest Phoenix.Sync

  describe "shape_from_params/[1,2]" do
    alias Electric.Client.ShapeDefinition

    test "returns a ShapeDefinition based on the request query params" do
      conn =
        conn(:get, "/my/path", %{
          "table" => "items",
          "namespace" => "my_app",
          "where" => "something = 'open'",
          "columns" => "id,name,value"
        })

      assert {:ok,
              %ShapeDefinition{
                table: "items",
                namespace: "my_app",
                where: "something = 'open'",
                columns: ["id", "name", "value"]
              }} = Phoenix.Sync.shape_from_params(conn)

      conn = conn(:get, "/my/path", %{"table" => "items"})

      assert {:ok,
              %ShapeDefinition{
                table: "items",
                namespace: nil,
                where: nil,
                columns: nil
              }} = Phoenix.Sync.shape_from_params(conn)

      conn = conn(:get, "/my/path", %{"where" => "true"})

      assert {:error, _} = Phoenix.Sync.shape_from_params(conn)

      conn =
        conn(:get, "/my/path", %{"table" => "items", "columns" => nil})

      assert {:ok, %ShapeDefinition{table: "items", columns: nil}} =
               Phoenix.Sync.shape_from_params(conn)
    end

    test "accepts a parameter map" do
      assert {:ok, %ShapeDefinition{table: "items"}} =
               Phoenix.Sync.shape_from_params(%{
                 "table" => "items",
                 "columns" => nil,
                 "where" => nil
               })

      assert {:error, _} = Phoenix.Sync.shape_from_params(%{})

      assert {:ok, %ShapeDefinition{table: "items"}} =
               Phoenix.Sync.shape_from_params(%{},
                 table: "items"
               )
    end

    test "allows for overriding specific attributes" do
      conn =
        conn(:get, "/my/path", %{
          "table" => "ignored",
          "namespace" => "ignored_as_well",
          "columns" => "ignored,also",
          "where" => "something = 'open'"
        })

      assert {:ok,
              %ShapeDefinition{
                table: "items",
                namespace: "my_app",
                where: "something = 'open'",
                columns: ["id", "name", "value"]
              }} =
               Phoenix.Sync.shape_from_params(conn,
                 table: "items",
                 namespace: "my_app",
                 columns: ["id", "name", "value"]
               )

      conn = conn(:get, "/my/path", %{"where" => "something = 'open'"})

      assert {:ok,
              %ShapeDefinition{
                table: "items",
                namespace: "my_app",
                where: "something = 'open'",
                columns: ["id", "name", "value"]
              }} =
               Phoenix.Sync.shape_from_params(conn,
                 table: "items",
                 namespace: "my_app",
                 columns: ["id", "name", "value"]
               )
    end
  end

  describe "interrupt/[1|2]" do
    alias Phoenix.Sync.PredefinedShape
    alias Phoenix.Sync.ShapeRequestRegistry

    test "interrupts matching tablename" do
      test_pid = self()

      # Spawn a separate process to simulate HTTP request process
      request_process =
        spawn_link(fn ->
          # Register shape from this process (simulates HTTP request)
          shape = PredefinedShape.new!(table: "threads", namespace: "public")
          {:ok, key} = ShapeRequestRegistry.register_shape(shape)

          # Wait for interruption signal
          receive do
            {:interrupt_shape, ^key, :server_interrupt} ->
              send(test_pid, {:got_interrupt, self()})
          end
        end)

      # Give process time to register
      Process.sleep(10)

      # Interrupt shapes (should target the spawned process)
      {:ok, count} = Phoenix.Sync.interrupt("threads")
      assert count == 1

      # Verify the spawned process got the interruption
      assert_receive {:got_interrupt, ^request_process}
    end

    test "shape matching w/params" do
      defns = [
        [[table: "todos"]],
        [[table: "todos"]],
        [[table: "todos", where: "completed = $1"]],
        [[table: "todos", where: "completed = $1", params: [true]]],
        [[table: "todos", where: "completed = $1", params: %{1 => true}]]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          where: "completed = $1",
          params: [true],
          columns: ["id", "title"]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Phoenix.Sync, :interrupt, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching w/namespace" do
      import Ecto.Query, only: [from: 2]

      defns = [
        [[table: "todos"]],
        [[namespace: "public", table: "todos"]],
        [[table: "todos", where: "completed = true"]],
        [Ecto.Query.put_query_prefix(Support.Todo, "public"), [where: "completed = true"]],
        [fn params -> params[:table] == "todos" end, []]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          namespace: "public",
          where: "completed = true",
          columns: ["id", "title", "completed"]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Phoenix.Sync, :interrupt, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching against Ecto.Query" do
      import Ecto.Query, only: [from: 2]

      defns = [
        [[table: "todos"]],
        [[namespace: "public", table: "todos"]],
        [from(t in Support.Todo, prefix: "public", where: t.completed == true), []],
        [fn params -> params[:table] == "todos" end, []]
      ]

      shape =
        PredefinedShape.new!(
          from(t in Support.Todo, prefix: "public", where: t.completed == true)
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Phoenix.Sync, :interrupt, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching w/o namespace" do
      defns = [
        ["todos"],
        [[table: "todos"]],
        [[table: "todos", where: "completed = true"]],
        [Support.Todo, [where: "completed = true"]]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          where: "completed = true",
          columns: ["id", "title", "completed"]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Phoenix.Sync, :interrupt, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end

    test "shape matching with function" do
      defns = [
        [fn %{table: table} -> table == "todos" end, []],
        [
          fn %{table: table, where: where} ->
            table == "todos" and where == "completed = true"
          end,
          []
        ]
      ]

      shape =
        PredefinedShape.new!(
          table: "todos",
          where: "completed = true",
          columns: ["id", "title", "completed"]
        )

      {:ok, key} = ShapeRequestRegistry.register_shape(shape)

      for args <- defns do
        assert {:ok, 1} = apply(Phoenix.Sync, :interrupt, args)

        assert_receive {:interrupt_shape, ^key, :server_interrupt}, 500
      end
    end
  end
end
