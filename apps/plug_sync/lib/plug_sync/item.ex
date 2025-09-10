defmodule PlugSync.Item do
  use Ecto.Schema

  schema "items" do
    field :name, :string
    field :value, :integer
    field :data, :string

    timestamps()
  end
end
