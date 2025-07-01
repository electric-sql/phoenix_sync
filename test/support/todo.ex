defmodule Support.Todo do
  use Ecto.Schema

  import Ecto.Changeset

  schema "todos" do
    field :title, :string
    field :completed, :boolean
  end

  def changeset(todo \\ %__MODULE__{}, data) do
    todo
    |> cast(data, [:id, :title, :completed])
    |> validate_required([:id, :title])
  end
end
