defmodule PlugSync.Repo.Migrations.CreateItems do
  use Ecto.Migration

  def change do
    create table(:items) do
      add :name, :string
      add :value, :integer

      timestamps()
    end
  end
end
