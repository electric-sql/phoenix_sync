defmodule PhoenixSyncExample.Cars.Model do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "models" do
    field :name, :string
    field :cost, :integer

    belongs_to :make, PhoenixSyncExample.Cars.Make, type: :binary_id

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(model, attrs) do
    model
    |> cast(attrs, [:name, :cost, :make_id])
    |> validate_required([:name, :cost, :make_id])
  end
end
