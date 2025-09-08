defmodule PlugSync.Repo.Migrations.CreateItems do
  use Ecto.Migration

  def change do
    create table(:items) do
      add :name, :string
      add :value, :integer
      add :data, :text

      timestamps()
    end
  end
end
