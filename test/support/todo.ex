defmodule Support.Todo do
  use Ecto.Schema

  schema "todos" do
    field :title, :string
    field :completed, :boolean
  end
end
