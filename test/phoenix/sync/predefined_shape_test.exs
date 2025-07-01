defmodule Phoenix.Sync.PredefinedShapeTest do
  use ExUnit.Case, async: true

  alias Phoenix.Sync.PredefinedShape
  alias Electric.Client.ShapeDefinition

  defmodule Cow do
    use Ecto.Schema

    schema "cows" do
      field :name, :string
      field :age, :integer
      field :breed, :string
    end

    def changeset(data \\ %__MODULE__{}, params) do
      import Ecto.Changeset

      data
      |> cast(params, [:name, :age, :breed])
      |> validate_number(:age, greater_than: 0)
      |> validate_required([:name])
      |> update_change(:breed, &String.downcase/1)
      |> validate_inclusion(:breed, ~w(holstein angus hereford jersey))
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
  end
end
