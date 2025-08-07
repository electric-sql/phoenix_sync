defmodule PhoenixSyncExample.Cars.Make do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "makes" do
    field :name, :string

    has_many :models, PhoenixSyncExample.Cars.Model, on_delete: :delete_all

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(make, attrs) do
    make
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end
