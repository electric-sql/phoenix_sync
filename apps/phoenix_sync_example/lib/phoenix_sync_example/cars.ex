defmodule PhoenixSyncExample.Cars do
  @moduledoc """
  The Cars context.
  """

  import Ecto.Query, warn: false
  alias PhoenixSyncExample.Repo

  alias PhoenixSyncExample.Cars.Make
  alias PhoenixSyncExample.Cars.Model

  @doc """
  Returns the list of cars.

  ## Examples

      iex> list_cars()
      [%Model{}, ...]

  """
  def list_cars do
    Repo.all(Model)
  end

  @doc """
  Gets a single car.

  Raises `Ecto.NoResultsError` if the Model does not exist.

  ## Examples

      iex> get_car!(123)
      %Model{}

      iex> get_car!(456)
      ** (Ecto.NoResultsError)

  """
  def get_car!(id), do: Repo.get!(Model, id)

  @doc """
  Creates a car.

  ## Examples

      iex> create_car(%{field: value})
      {:ok, %Model{}}

      iex> create_car(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_car(attrs \\ %{}) do
    %Model{}
    |> Model.changeset(attrs)
    |> Repo.insert()
  end

  def create_make(attrs \\ %{}) do
    %Make{}
    |> Make.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a car.

  ## Examples

      iex> update_car(car, %{field: new_value})
      {:ok, %Model{}}

      iex> update_car(car, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_car(%Model{} = car, attrs) do
    car
    |> Model.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a car.

  ## Examples

      iex> delete_car(car)
      {:ok, %Model{}}

      iex> delete_car(car)
      {:error, %Ecto.Changeset{}}

  """
  def delete_car(%Model{} = car) do
    Repo.delete(car)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking car changes.

  ## Examples

      iex> change_car(car)
      %Ecto.Changeset{data: %Model{}}

  """
  def change_car(%Model{} = car, attrs \\ %{}) do
    Model.changeset(car, attrs)
  end
end
