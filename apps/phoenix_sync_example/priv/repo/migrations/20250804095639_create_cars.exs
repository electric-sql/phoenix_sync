defmodule PhoenixSyncExample.Repo.Migrations.CreateCars do
  use Ecto.Migration

  def change do
    create table(:makes, primary_key: false) do
      add(:id, :binary_id, primary_key: true)
      add(:name, :string)

      timestamps(type: :utc_datetime)
    end

    create table(:models, primary_key: false) do
      add(:id, :binary_id, primary_key: true)
      add(:name, :string)
      add(:cost, :integer)

      add(:make_id, references(:makes, type: :binary_id, on_delete: :delete_all), null: false)

      timestamps(type: :utc_datetime)
    end
  end
end
