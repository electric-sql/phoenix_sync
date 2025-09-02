defmodule Phoenix.Sync.PredefinedShapeTest do
  use ExUnit.Case, async: true

  alias Phoenix.Sync.PredefinedShape
  alias Electric.Client.ShapeDefinition

  defmodule Cow do
    use Ecto.Schema

    schema "cows" do
      field :name, :string
      field :age, :integer
      field :breed, Ecto.Enum, values: [:holstein, :angus, :hereford, :jersey]
    end

    def changeset(data \\ %__MODULE__{}, params) do
      import Ecto.Changeset

      data
      |> cast(params, [:name, :age, :breed])
      |> validate_number(:age, greater_than: 0)
      |> validate_required([:name, :breed])
    end
  end

  describe "new!/2" do
    test "raises if passed unknown options" do
      assert_raise ArgumentError, fn ->
        PredefinedShape.new!(table: "here", sheep: "baa")
      end
    end

    test "raises if passed invalid options" do
      invalid = [
        [],
        [where: "something = true"],
        [table: "here", replica: :invalid],
        [table: "here", storage: :invalid],
        [table: "here", params: :invalid],
        [table: "here", columns: :invalid],
        [table: "here", namespace: :invalid],
        [table: "here", where: :invalid],
        [table: "here", live: :invalid],
        [table: "here", errors: :invalid]
      ]

      for opts <- invalid do
        assert_raise NimbleOptions.ValidationError, fn ->
          PredefinedShape.new!(opts)
        end
      end
    end

    test "accepts keyword-based shape definition" do
      ps =
        PredefinedShape.new!(
          table: "todos",
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          columns: ["id", "title"],
          storage: %{compaction: :disabled}
        )

      assert PredefinedShape.to_client_params(ps) == %{
               "params[1]" => "true",
               "replica" => "full",
               "table" => "test.todos",
               "where" => "completed = $1",
               "columns" => "id,title"
             }

      assert PredefinedShape.to_api_params(ps) |> Enum.sort() ==
               Enum.sort(
                 table: "todos",
                 namespace: "test",
                 where: "completed = $1",
                 params: %{"1" => "true"},
                 replica: :full,
                 columns: ["id", "title"],
                 storage: %{compaction: :disabled}
               )
    end

    test "accepts Ecto schema" do
      ps = PredefinedShape.new!(Cow, storage: %{compaction: :disabled})

      assert PredefinedShape.to_client_params(ps) == %{
               "columns" => "id,name,age,breed",
               "table" => "cows"
             }

      assert PredefinedShape.to_api_params(ps) |> Enum.sort() ==
               Enum.sort(
                 table: "cows",
                 columns: ["id", "name", "age", "breed"],
                 storage: %{compaction: :disabled}
               )
    end

    test "accepts Ecto schema plus opts" do
      ps =
        PredefinedShape.new!(
          Cow,
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          columns: ["id", "title"],
          storage: %{compaction: :disabled}
        )

      assert PredefinedShape.to_client_params(ps) == %{
               "columns" => "id,title",
               "params[1]" => "true",
               "replica" => "full",
               "table" => "test.cows",
               "where" => "completed = $1"
             }
    end

    test "changeset function plus opts" do
      ps =
        PredefinedShape.new!(
          &Cow.changeset/1,
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          storage: %{compaction: :disabled},
          live: false,
          errors: :stream
        )

      assert PredefinedShape.to_client_params(ps) == %{
               "columns" => "id,name,breed,age",
               "params[1]" => "true",
               "replica" => "full",
               "table" => "test.cows",
               "where" => "completed = $1"
             }

      assert {%{__struct__: ShapeDefinition}, [live: false, errors: :stream]} =
               PredefinedShape.to_stream_params(ps)
    end

    @tag :transform
    test "transform function is accepted as mfa" do
      ps =
        PredefinedShape.new!(
          Cow,
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          columns: ["id", "title"],
          transform: {__MODULE__, :map_cow_dupe, []},
          storage: %{compaction: :disabled}
        )

      assert fun = PredefinedShape.transform_fun(ps)
      assert is_function(fun, 1)

      assert [:msg, :msg] = fun.(:msg)
    end

    @tag :transform
    test "transform function is accepted as a capture" do
      ps =
        PredefinedShape.new!(
          Cow,
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          columns: ["id", "title"],
          transform: &map_cow/1,
          storage: %{compaction: :disabled}
        )

      assert fun = PredefinedShape.transform_fun(ps)
      assert is_function(fun, 1)

      assert [:msg] = fun.(:msg)
    end

    @tag :transform
    test "ecto schema modules are accepted as a transform argument" do
      ps =
        PredefinedShape.new!(
          Cow,
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          transform: Cow
        )

      assert fun = PredefinedShape.transform_fun(ps)
      assert is_function(fun, 1)

      assert [
               %{
                 "key" => "key",
                 "headers" => %{"operation" => "insert"},
                 "value" => %Cow{name: "Daisy", age: 12, breed: :jersey}
               }
             ] =
               fun.(%{
                 "key" => "key",
                 "headers" => %{"operation" => "insert"},
                 "value" => %{"name" => "Daisy", "age" => 12, "breed" => "jersey"}
               })
    end

    @tag :transform
    test "transform function is wrapped to return a list" do
      ps =
        PredefinedShape.new!(
          Cow,
          namespace: "test",
          where: "completed = $1",
          params: [true],
          replica: :full,
          columns: ["id", "title"],
          transform: {__MODULE__, :map_cow, []},
          storage: %{compaction: :disabled}
        )

      assert fun = PredefinedShape.transform_fun(ps)
      assert is_function(fun, 1)

      assert [:msg] = fun.(:msg)
    end

    @tag :transform
    test "shape with no transform fun" do
      ps = PredefinedShape.new!(table: "cows")

      assert nil == PredefinedShape.transform_fun(ps)
    end

    @tag :transform
    test "transform_fun accepts and returns nil" do
      assert nil == PredefinedShape.transform_fun(nil)
    end
  end

  def map_cow(msg), do: msg
  def map_cow_dupe(msg), do: [msg, msg]
end
